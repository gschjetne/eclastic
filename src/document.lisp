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

(defpackage :eclastic.document
  (:use :cl
        :eclastic.generic
        :eclastic.server
        :eclastic.util)
  (:import-from :anaphora
                :aand
                :it)
  (:import-from :alexandria
                :with-gensyms)
  (:import-from :yason
                :with-object
                :with-output-to-string*
                :encode-object-element
                :encode-slots)
  (:export :<document>
           :document-id
           :document-source
           :version
           :version-conflict
           :document-exists
           :routing
           :parent-of
           :hash-to-document
           :document-not-found
           :document-with-id
           :document-by-id
           :define-source-accessors))

(in-package :eclastic.document)

(defclass <document> (<type>)
  ((id :initarg :id
       :accessor document-id)
   (source :initarg :source
           :initform nil
           :accessor document-source)
   (version :initarg :version
            :initform nil
            :accessor version)
   (routing :initarg :routing
            :initform nil
            :reader routing)
   (shard-preference :initarg :shard-preference
                     :initform nil)
   (refresh-when-get :initarg :refresh
                     :initform nil)
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

(define-condition document-not-found (warning) ())

(define-condition version-conflict (error) ())

(define-condition document-exists (error) ())

(defmethod get-query-params ((this <document>))
  (with-slots (routing shard-preference refresh-when-get version) this
    (let ((parameters nil))
      (when routing
        (push (cons "routing" routing)
              parameters))
      (when shard-preference
        (push (cons "preference" shard-preference)
              parameters))
      (when refresh-when-get
        (push (cons "refresh" refresh-when-get)
              parameters))
      (when version
        (push (cons "version" (format nil "~D" version))
              parameters)))))

;; CRUD methods

(defmethod get* ((place <type>) (document <document>))
  (let ((result
         (send-request
          (format nil "~A/~A" (get-uri place) (document-id document))
          :get
          :parameters (get-query-params document))))
    (when (aand (gethash "status" result) (= it 409))
      (error 'version-conflict))
    (unless (json-true-p (gethash "found" result))
      (warn 'document-not-found))
    (hash-to-document result)))

(defmethod index ((place <type>) (document <document>))
  (let ((result
         (send-request
          (format nil "~A/~A" (get-uri place) (document-id document))
          :put
          :data (with-output-to-string (s)
                  (yason:encode (document-source document) s)))))
    (hash-to-document result)))

(defmethod create ((place <type>) (document <document>))
  (multiple-value-bind (result status)
      (send-request
       (format nil "~A/~A/_create" (get-uri place) (document-id document))
       :put
       :data (with-output-to-string (s)
               (yason:encode (document-source document) s)))
    (if (= 409 status)
        (error 'document-exists)
        (hash-to-document result))))

(defmethod update ((place <type>) (document <document>)
                   &key script upsert detect-noop)
  (multiple-value-bind (result status)
      (send-request
       (format nil "~A/~A/_update" (get-uri place) (document-id document))
       :post
       :data (with-output-to-string* ()
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
                 (encode-object-element* "detect_noop" detect-noop)))
       :parameters (get-query-params document))
    (when (= 404 status) (warn 'document-not-found))
    (hash-to-document result)))

(defmethod delete* ((place <type>) (document <document>))
  (let ((result
         (send-request
          (format nil "~A/~A" (get-uri place) (document-id document))
          :delete
          :parameters (get-query-params document))))
    (unless (gethash "found" result)
      (warn 'document-not-found))))

;; Utilities

(defun document-with-id (id &key routing preference refresh version source)
  (make-instance '<document>
                 :id id
                 :routing routing
                 :shard-preference (etypecase preference
                               (keyword (ecase preference
                                          (:primary "_primary")
                                          (:local "_local")))
                               (string preference)
                               (null nil))
                 :refresh refresh
                 :version version
                 :source source))

(defun document-by-id (place id &key routing preference refresh version)
  (get* place (document-with-id id
                                :routing routing
                                :preference preference
                                :refresh refresh
                                :version version)))

(defmacro define-source-accessors ((arg class) &body bindings)
  `(progn ,@(loop for binding in bindings collect
                 (destructuring-bind (method-name hash-key &optional default)
                     binding
                   (let ((setter (intern (format nil "SET-~A" method-name))))
                     `(progn (defgeneric ,method-name (,arg))
                             (defgeneric ,setter (,arg val))
                             (defmethod ,method-name ((,arg ,class))
                               (unless (document-source ,arg)
                                 (error "The document does not have a document source"))
                               (gethash ,hash-key (document-source ,arg)
                                        ,default))
                             (defmethod ,setter ((,arg ,class) val)
                               (unless (document-source ,arg)
                                 (cerror "Create source hash table"
                                         "The document does not have a document source")
                                 (setf (document-source ,arg)
                                       (make-hash-table :test 'equal)))
                               (setf (gethash ,hash-key
                                              (document-source ,arg)) val))
                             (defsetf ,method-name ,setter)))))))
