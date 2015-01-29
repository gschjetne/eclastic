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

(defpackage :eclastic.util
  (:use :cl)
  (:import-from :yason
                :parse
                :encode-object
                :encode-object-element
                :with-object-element)
  (:import-from :drakma
                :http-request
                :*text-content-types*)
  (:export :encode-object-element*
           :with-object-element*
           :ensure-mutually-exclusive
           :inspect-json
           :json-true-p
           :to-json-boolean
           :hash-table-keyword-plist
           :comma-join
           :send-request))

(in-package :eclastic.util)

(defmacro encode-object-element* (key value)
  "Encode key-value pair only if VALUE is non-NIL"
  `(when ,value
     (encode-object-element ,key ,value)))

(defmacro with-object-element* ((key predicate) &body body)
  "Encode key-body only if PREDICATE is non-NIL"
  `(when ,predicate
     (with-object-element (,key) ,@body)))

(defun inspect-json (object &optional (stream *standard-output*))
  (yason:with-output (stream :indent t)
    (encode-object object)))

(defun json-true-p (yason-bool)
  (if (eq yason-bool 'yason:false)
      nil
      yason-bool))

(defun to-json-boolean (value)
  (if value
      'yason:true
      'yason:false))

(defmacro ensure-mutually-exclusive (a b)
  `(progn
     (when (and ,a ,b)
       (error (format nil "~A and ~A are mutually exclusive"
                      (quote ,a) (quote ,b))))
     (unless (or ,a ,b)
       (error (format nil "Either ~A or ~A must be non-NIL"
                      (quote ,a) (quote ,b))))))

(defun hash-table-keyword-plist (table)
  (let ((plist nil))
    (maphash (lambda (key value)
               (setf (getf plist (intern (string-upcase key)
                                         :keyword)) value)) table)
    plist))

(defun comma-join (list)
  (with-output-to-string (s)
    (funcall (formatter "~{~A~^,~}") s list)))

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
      (declare (ignore headers uri stream reason))
      (unwind-protect
           (if (= status 400)
               (error (gethash "error" (parse body)))
               (values (parse body) status))
        (when closep
          (close body))))))
