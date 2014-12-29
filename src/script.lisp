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

(defpackage :eclastic.script
  (:use :cl
        :eclastic.util)
  (:import-from :alexandria
                :alist-hash-table)
  (:import-from :yason
                :encode-slots
                :encode-object-element)
  (:export :<script>
           :define-script
           :encode-script))

(in-package :eclastic.script)

(defclass <script> ()
  ((body :initarg :script-body
         :reader script-body)
   (parameters :initarg :script-parameters
               :reader script-parameters)))

(defun define-script (body &rest parameters)
  (make-instance '<script>
                 :body body
                 :script-parameters (alist-hash-table parameters
                                                      :test 'equal)))

(defmethod encode-slots progn ((this <script>))
  (encode-object-element "script" (script-body this))
  (encode-object-element "params" (script-parameters this)))

(defun encode-script (script)
  (when script
    (encode-slots script)))
