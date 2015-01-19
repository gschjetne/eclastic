;; Copyright © 2014 FMAP SVERIGE AB

;; This file is part of Eclastic

;; Eclastic is free software: you can redistribute it and/or modify
;; it under the terms of the GNU Lesser General Public License as
;; published by the Free Software Foundation, either version 3 of the
;; License, or (at your option) any later version.

;; Eclastic is distributed in the hope that it will be useful, but
;; WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
;; Lesser General Public License for more details.

;; You should have received a copy of the GNU Lesser General Public
;; License along with Eclastic.  If not, see
;; <http://www.gnu.org/licenses/>.

(in-package :cl-user)

(defpackage :eclastic.search
  (:use :cl
        :eclastic.generic
        :eclastic.server
        :eclastic.util
        :eclastic.document)
  (:import-from :yason
                :encode
                :encode-object
                :encode-array-element
                :encode-slots
                :with-array
                :with-output
                :with-output-to-string*
                :with-object
                :with-object-element)
  (:import-from :anaphora
                :awhen
                :it)
  (:export :<search>
           :new-search
           :sort-by))

(in-package :eclastic.search)

(defclass <search> ()
  ((query :initarg :query
          :reader query)
   (timeout :initarg :timeout
            :reader timeout)
   (from :initarg :from
         :reader from)
   (size :initarg :size
         :reader size)
   (search-type :initarg :search-type)
   (query-cache :initarg :query-cache)
   (terminate-after :initarg :terminate-after
                    :reader terminate-after)
   (aggregations :initarg :aggregations
                 :reader aggregations)
   (suggestions :initarg :suggestions
                :reader suggestions)
   (sort :initarg :sort-by
         :reader sort-by-list)
   (fields :initarg :fields
           :reader return-fields)))

(defmethod get-query-params ((this <search>))
  (awhen (slot-value this 'search-type)
    (cons (cons "search_type" it)
          (awhen (slot-value this 'query-cache)
            (list (cons "query_cache" it))))))

(defclass <sort-by> ()
  ((field :initarg :field
          :reader field)
   (order :initarg :order
          :reader sort-order)
   (mode :initarg :mode
         :reader sort-mode)))

(defmethod encode ((this <sort-by>) &optional (stream *standard-output*))
  (with-output (stream)
    (with-object ()
        (with-object-element ((field this))
          (with-object ()
            (encode-object-element* "order" (sort-order this))
            (encode-object-element* "mode" (sort-mode this)))))))

(defun sort-by (field &key order mode)
  (if (or order mode)
      (make-instance '<sort-by>
                     :field field
                     :order (when order
                              (ecase order
                                (:ascending "asc")
                                (:descending "desc")))
                     :mode (when mode
                             (ecase mode
                               (:min "min")
                               (:max "max")
                               (:sum "sum")
                               (:avg "avg"))))
      field))

(defmethod encode-slots progn ((this <search>))
  (with-object-element* ("query" (query this))
    (encode-object (query this)))
  (encode-object-element* "timeout" (timeout this))
  (encode-object-element* "from" (from this))
  (encode-object-element* "size" (size this))
  (encode-object-element* "terminate_after" (terminate-after this))
  (with-object-element* ("aggregations" (aggregations this))
    (with-object ()
      (loop for pair in (aggregations this) do
           (with-object-element ((car pair))
             (encode-object (cdr pair))))))
  (with-object-element* ("suggest" (suggestions this))
    (with-object ()
      (loop for pair in (suggestions this) do
           (with-object-element ((car pair))
             (encode-object (cdr pair))))))
  (with-object-element* ("sort" (sort-by-list this))
    (with-array ()
      (loop for sort-option in (sort-by-list this) do
           (encode-array-element sort-option))))
  (let ((fields (return-fields this)))
    (with-object-element* ("fields" fields)
      (with-array ()
        (unless (eq fields :none)
          (loop for field in fields do
               (encode-array-element field)))))))

(defun new-search (query &key aggregations timeout
                           from size search-type
                           query-cache terminate-after
                           suggestions sort fields)
  (make-instance '<search>
                 :query query
                 :timeout timeout
                 :from from
                 :size size
                 :search-type (when search-type
                                (ecase search-type
                                  (:dfs-query-then-fetch
                                   "dfs_query_then_fetch")
                                  (:dfs-query-and-fetch
                                   "dfs_query_and_fetch")
                                  (:query-then-fetch
                                   "query_then_fetch")
                                  (:query-and-fetch
                                   "query_then_fetch")
                                  (:count
                                   "count")
                                  (:scan
                                   "scan")))
                 :query-cache (when query-cache
                                (ecase query-cache
                                  (:enable "true")
                                  (:disable "false")))
                 :terminate-after terminate-after
                 :aggregations aggregations
                 :suggestions suggestions
                 :sort-by (etypecase sort
                            (cons sort)
                            (string (list sort))
                            (<sort-by> (list sort))
                            (null nil))
                 :fields fields))

;; CRUD methods

(defmethod get* ((place <server>) (query <search>))
  (let* ((result
          (send-request (format nil "~A/_search"
                                (get-uri place))
                        :get
                        :data (with-output-to-string* ()
                                (encode-object query))
                        :parameters (get-query-params query)))
         (hits (gethash "hits" result))
         (shards (gethash "_shards" result)))
    (values (mapcar #'hash-to-document (gethash "hits" hits))
            (gethash "aggregations" result)
            (list :hits (gethash "total" hits)
                  :shards (hash-table-keyword-plist shards)
                  :timed-out (gethash "timed_out" result)
                  :took (gethash "took" result)))))

(defmethod delete* ((place <type>) (query <search>))
  (let ((result (send-request (format nil "~A/_query"
                                      (get-uri place))
                              :delete
                              :data (with-output-to-string* ()
                                      (encode-object query)))))
    (apply #'values
           (loop for index being each hash-key in
                (gethash "_indices" result) using
                (hash-value value) collect
                (list :index index
                      :shards
                      (hash-table-keyword-plist (gethash "_shards" value)))))))
