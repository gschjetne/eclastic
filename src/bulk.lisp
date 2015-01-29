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

(defpackage :eclastic.bulk
  (:use :cl
        :eclastic.generic
        :eclastic.script
        :eclastic.document
        :eclastic.server
        :eclastic.util)
  (:import-from :yason
                :with-output
                :with-object
                :with-object-element
                :encode
                :encode-object-element
                :encode-slots)
  (:import-from :alexandria
                :with-gensyms)
  (:export :<bulk>
           :with-bulk-documents))

(in-package :eclastic.bulk)

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

(defmethod index ((bulk <bulk>) (document <document>))
  (with-output ((get-bulk-stream bulk))
    (with-object ()
      (with-object-element ("index")
        (with-object ()
          (encode-object-element* "_index" (index-name document))
          (encode-object-element* "_type"  (type-name document))
          (encode-object-element* "_id" (document-id document))
          (encode-object-element* "_version" (version document))
          (encode-object-element* "_routing" (routing document))
          (encode-object-element* "_parent" (parent-of document))))))
  (fresh-line (get-bulk-stream bulk))
  (encode (document-source document) (get-bulk-stream bulk))
  (fresh-line (get-bulk-stream bulk)))

(defmethod update ((bulk <bulk>) (document <document>)
                   &key script upsert detect-noop)
  (with-output ((get-bulk-stream bulk))
    (with-object ()
      (with-object-element ("update")
        (with-object ()
          (encode-object-element* "_index" (index-name document))
          (encode-object-element* "_type"  (type-name document))
          (encode-object-element* "_id" (document-id document))
          (encode-object-element* "_version" (version document))
          (encode-object-element* "_routing" (routing document))
          (encode-object-element* "_parent" (parent-of document)))))
    (fresh-line (get-bulk-stream bulk))
    (with-object ()
      (if script
          (encode-slots script)
          (encode-object-element "doc" (document-source document)))
      (etypecase upsert
        (null nil)
        (<document>
         (encode-object-element "upsert" (document-source upsert)))
        (hash-table
         (encode-object-element "upsert" upsert))
        (boolean
         (if script
             (encode-object-element "scripted_upsert" t)
             (encode-object-element "doc_as_upsert" t))))
      (encode-object-element* "detect_noop" detect-noop))
    (fresh-line (get-bulk-stream bulk))))

(defmethod post ((place <server>) (bulk <bulk>))
  (let* ((data (get-bulk-string bulk))
         (result
          (unless (zerop (length data))
            (send-request (format nil "~A/_bulk" (get-uri place))
                          :post :data data))))
    (when result
      (values (mapcar (lambda (item)
                        (car (loop for action being each hash-key of item collect
                                  (cons (hash-to-document (gethash action item))
                                        (intern (string-upcase action) :keyword)))))
                      (gethash "items" result))
              (list :errors (gethash "errors" result)
                    :took (gethash "took" result))))))

;; Utilities

(defmacro with-bulk-documents ((var place) &body documents)
  (with-gensyms (result)
    `(let ((,var (make-instance '<bulk>))
           (,result))
       (unwind-protect
            (progn ,@documents
                   (setf ,result (multiple-value-list (post ,place ,var))))
         (close ,var))
       (apply #'values ,result))))
