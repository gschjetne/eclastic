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

(defpackage :eclastic.generic
  (:use :cl)
  (:export :get*
           :index
           :create
           :read*
           :update
           :delete*
           :post
           :get-uri
           :get-query-params))

(in-package :eclastic.generic)

(defgeneric get* (place contents)
  (:documentation "Retrieve documents from the database"))

(defgeneric index (place contents)
  (:documentation "Index documents in the database"))

(defgeneric create (place contents))

(defgeneric update (place contents &key script upsert detect-noop))

(defgeneric delete* (place contents)
  (:documentation "Delete documents from the database"))

(defgeneric post (place contents))

(defgeneric get-uri (object))

(defgeneric get-query-params (object))
