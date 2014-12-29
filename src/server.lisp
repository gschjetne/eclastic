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

(defpackage :eclastic.server
  (:use :cl
        :eclastic.generic)
  (:export :<server>
           :host
           :port
           :<index>
           :index-name
           :<type>
           :type-name))

(in-package :eclastic.server)

(defclass <server> ()
  ((host :initarg :host
         :initform "localhost"
         :reader host)
   (port :initarg :port
         :initform 9200
         :reader port)))

(defmethod get-uri ((this <server>))
  (format nil "http://~A:~A"
          (host this)
          (port this)))

(defclass <index> (<server>)
  ((index-name :initarg :index
               :initform nil
               :reader index-name)))

(defmethod get-uri ((this <index>))
  (format nil "~A/~A"
               (call-next-method)
               (index-name this)))

(defclass <type> (<index>)
  ((type-name :initarg :type
              :initform nil
              :reader type-name)))

(defmethod get-uri ((this <type>))
  (format nil "~A/~A"
               (call-next-method)
               (type-name this)))

