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
(defpackage :eclastic.suggest
  (:use :cl
        :eclastic.util)
  (:import-from :yason
                :encode
                :encode-slots
                :encode-object
                :encode-object-element
                :with-array
                :encode-array-elements
                :with-object
                :with-object-element
                :with-output-to-string*
                :*json-output*)
  (:export :<term>)))

(in-package :eclastic.suggest)

(defclass <suggester> ()
  ((text :initarg :text
         :reader text)))

(defmethod encode-slots progn ((this <suggester>))
  (encode-object-element "text" (text this)))

(defclass <term> (<suggester>)
  ())
