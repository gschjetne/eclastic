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

(defpackage :cl-eclastic-system (:use :cl :asdf))
(in-package :cl-eclastic-system)

(defsystem eclastic
  :name "Eclastic"
  :author "Grim Schjetne <grim@schjetne.se"
  :description "Elasticsearch client library"
  :license "LGPLv3+"
  :depends-on (:drakma
               :yason
               :flexi-streams)
  :components
  ((:module "src"
            :serial t
            :components
            ((:file "util")
             (:file "query")
             (:file "aggregations")
             (:file "main")))))
