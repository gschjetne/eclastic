;; Copyright © 2014 FMAP SVERIGE AB
;;           © 2014 Olof-Joachim Frahm <olof@macrolet.net>

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
(defpackage :eclastic
  (:use :cl
        :eclastic.util)
  (:import-from :yason
                :parse
                :encode
                :encode-slots
                :encode-object
                :encode-object-element
                :with-object
                :with-object-element
                :with-output
                :with-output-to-string*
                :*json-output*)
  (:import-from :drakma
                :http-request
                :*text-content-types*)
  (:import-from :flexi-streams
                :octets-to-string)
  (:import-from :anaphora
                :awhen
                :it)
  (:import-from :alexandria
                :with-gensyms)
  (:export :get*
           :new-search
           :document-by-id
           :document-not-found
           :document-id
           :document-source
           :<document>
           :<server>
           :<index>
           :<type>
           :<search>))

(in-package :eclastic)

(defclass <server> ()
  ((host :initarg :host
         :initform "localhost"
         :reader host)
   (port :initarg :port
         :initform 9200
         :reader port)))

(defgeneric get-uri (entity))

(defmethod get-uri ((this <server>))
  (format nil "http://~A:~A"
          (host this)
          (port this)))

(defun send-request (uri method &key data parameters)
  (let ((*text-content-types*
          '(("application" . "json"))))
    (multiple-value-bind (body status headers uri stream closep reason)
        (http-request uri
                      :method method
                      :content data
                      :content-type "application/json"
                      :external-format-in :utf-8
                      :external-format-out :utf-8
                      :parameters parameters
                      :want-stream T)
      (declare (ignore status headers uri stream reason))
      (unwind-protect
           (parse body)
        (when closep
          (close body))))))

(defclass <index> (<server>)
  ((index-name :initarg :index
               :initform nil
               :reader index-name)))

(defclass <type> (<index>)
  ((type-name :initarg :type
              :initform nil
              :reader type-name)))

(defmethod get-uri ((this <index>))
  (format nil "~A/~A"
               (call-next-method)
               (index-name this)))

(defmethod get-uri ((this <type>))
  (format nil "~A/~A"
               (call-next-method)
               (type-name this)))

(defgeneric get* (place contents))

(defgeneric post* (place contents))

(defclass <document> (<type>)
  ((id :initarg :id
       :reader document-id)
   (source :initarg :source
           :initform nil
           :accessor document-source)
   (version :initarg :version
            :initform nil
            :accessor version)
   (routing :initarg :routing
            :initform nil
            :reader routing)
   (parent :initarg :parent
           :initform nil
           :reader parent-of)))

(defmethod print-object ((obj <document>) stream)
   (print-unreadable-object (obj stream :type t :identity t)
     (format stream "/~A/~A/~A"
             (index-name obj)
             (type-name obj)
             (document-id obj))))

(defun hash-to-document (hash-table &key host port)
  (make-instance '<document>
                 :index (gethash "_index" hash-table)
                 :type (gethash "_type" hash-table)
                 :id (gethash "_id" hash-table)
                 :version (gethash "_version" hash-table)
                 :source (gethash "_source" hash-table)
                 :routing (gethash "_routing" hash-table)
                 :parent (gethash "_parent" hash-table)
                 :host host
                 :port port))

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
                :reader suggestions)))

(defgeneric get-query-params (object))

(defmethod get-query-params ((this <search>))
  (awhen (slot-value this 'search-type)
    (cons (cons "search_type" it)
          (awhen (slot-value this 'query-cache)
            (list (cons "query_cache" it))))))

(defun new-search (query &key aggregations timeout
                           from size search-type
                           query-cache terminate-after
                           suggestions)
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
                 :suggestions suggestions))

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
             (encode-object (cdr pair)))))))

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
                  :shards (list :total (gethash "total" shards)
                                :failed (gethash "failed" shards)
                                :successful (gethash "successful" shards))
                  :timed-out (gethash "timed_out" result)
                  :took (gethash "took" result)))))

(defmethod get* ((place <type>) (document <document>))
  (let ((result
         (send-request
          (format nil "~A/~A" (get-uri place) (document-id document))
          :get)))
    (unless (gethash "found" result)
      (warn 'document-not-found))
    (hash-to-document result)))

(define-condition document-not-found (warning)
  ())

(defun document-by-id (place id)
  (get* place (make-instance '<document> :id id)))

(defclass <bulk> ()
  ((bulk-stream :initform (make-string-output-stream)
                :reader get-bulk-stream)))

(defmethod close ((this <bulk>) &key abort)
  (close (get-bulk-stream this) :abort abort))

(defgeneric get-bulk-string (bulk))

(defmethod get-bulk-string ((this <bulk>))
  (when (open-stream-p (get-bulk-stream this))
    (close this)
    (get-output-stream-string (get-bulk-stream this))))

(defgeneric put-document (bulk document &optional action))

(defmethod put-document ((bulk <bulk>) (document <document>) &optional (action :index))
  (with-output ((get-bulk-stream bulk))
    (with-object ()
      (with-object-element ((ecase action
                              (:index "index")
                              (:create "create")
                              (:delete "delete")
                              (:update "update")))
        (with-object ()
          (encode-object-element* "_index" (index-name document))
          (encode-object-element* "_type"  (type-name document))
          (encode-object-element* "_id" (document-id document))
          (encode-object-element* "_version" (version document))
          (encode-object-element* "_routing" (routing document))
          (encode-object-element* "_parent" (parent-of document))))))
  (fresh-line (get-bulk-stream bulk))
  (unless (eq action :delete)
    (encode (document-source document) (get-bulk-stream bulk))
    (fresh-line (get-bulk-stream bulk))))

(defmethod post ((place <server>) (bulk <bulk>))
  (let ((result
         (send-request (format nil "~A/_bulk" (get-uri place))
          :post :data (get-bulk-string bulk))))
    (values (mapcar (lambda (item)
                      (car (loop for action being each hash-key of item collect
                                (cons (hash-to-document (gethash action item))
                                      (intern (string-upcase action) :keyword)))))
                    (gethash "items" result))
            (list :errors (gethash "errors" result)
                  :took (gethash "took" result)))))

(defmacro with-bulk-documents ((var place) &body documents)
  (with-gensyms (result)
    `(let ((,var (make-instance '<bulk>))
           (,result))
       (unwind-protect (progn ,@documents
                              (setf ,result (multiple-value-list (post ,place ,var))))
         (close ,var))
       (apply #'values ,result))))

