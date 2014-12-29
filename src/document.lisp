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
  (:export :<document>
           :document-id
           :document-source
           :version
           :routing
           :parent-of
           :hash-to-document
           :document-not-found
           :document-with-id
           :document-by-id))

(in-package :eclastic.document)

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

(define-condition document-not-found (warning) ())

;; CRUD methods

(defmethod get* ((place <type>) (document <document>))
  (let ((result
         (send-request
          (format nil "~A/~A" (get-uri place) (document-id document))
          :get)))
    (unless (gethash "found" result)
      (warn 'document-not-found))
    (hash-to-document result)))

;; Utility functions

(defun document-with-id (id)
  (make-instance '<document> :id id))

(defun document-by-id (place id)
  (get* place (document-with-id id)))
