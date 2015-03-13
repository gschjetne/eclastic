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

(defpackage :eclastic.query
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
  (:export :<match>
           :<bool>
           :<boosting>
           :<filtered>
           :<terms>
           :<match-all>
           :<ids>
           :<range>
           :<has-child>
           :<has-parent>
           :<prefix>
           :match
           :bool
           :boosting
           :filtered
           :terms
           :match-all
           :ids
           :range
           :has-child
           :has-parent
           :prefix))

(in-package :eclastic.query)

(defclass <query> () ())

(defclass <filter> ()
  ((in-filter-position-p :initform nil
                         :accessor in-filter-position-p)))

(defgeneric encode-subquery (subquery))

(defmethod encode-subquery ((this <query>))
  (encode-object this))

(defmethod encode-subquery ((this cons))
  (with-array ()
    (loop for subquery in this do
         (encode-subquery subquery))))

(defclass <string-query> (<query>)
  ((query-string :initarg :query-string
                 :reader query-string)))

(defclass <list-query> (<query>)
  ((query-list :initarg :query-list
               :reader query-list)))

(defclass <fuzzy-query> (<query>)
  ((fuzziness :initarg :fuzziness
              :reader fuzziness)))

(defclass <boolean-query> (<query>)
  ((operator :initarg :operator
             :reader operator)))

(defclass <field-query> (<query>)
  ((search-field :initarg :search-field
                 :reader search-field)))

(defclass <zero-terms-query> (<query>)
  ((zero-terms-query :initarg :zero-terms-query
                     :reader zero-terms-query)))

(defclass <cutoff-frequency-query> (<query>)
  ((cutoff-frequency :initarg :cutoff-frequency
                     :reader cutoff-frequency)))

(defclass <boost-query> (<query>)
  ((boost :initarg :boost
          :reader boost)))

(defclass <minimum-should-match-query> (<query>)
  ((minimum-should-match :initarg :minimum-should-match
                         :reader minimum-should-match)))

(defclass <score-mode-query> (<query>)
  ((score-mode :initarg :score-mode
               :reader score-mode)))

(defclass <match> (<string-query>
                   <fuzzy-query>
                   <boolean-query>
                   <field-query>
                   <zero-terms-query>
                   <cutoff-frequency-query>
                   <boost-query>)
  ((match-type :initarg :match-type
               :reader match-type)))

(defmethod encode-slots progn ((this <match>))
  (with-object-element ((match-type this))
    (with-object ()
      (with-object-element ((search-field this))
        (with-object ()
          (encode-object-element "query" (query-string this))
          (encode-object-element* "operator" (operator this))
          (encode-object-element* "zero_terms_query"
                                  (zero-terms-query this))
          (encode-object-element* "cutoff_frequency"
                                  (cutoff-frequency this))
          (encode-object-element* "fuzziness"
                                  (fuzziness this))
          (encode-object-element* "boost" (boost this)))))))

(defun match (query-string field &key
                                   operator
                                   (type :match)
                                   fuzziness
                                   zero-terms-query
                                   cutoff-frequency
                                   boost)
  (make-instance '<match>
                 :query-string query-string
                 :search-field field
                 :operator (when operator
                             (ecase operator
                               (:or "or")
                               (:and "and")))
                 :match-type (when type
                               (ecase type
                                 (:match "match")
                                 (:match-phrase "match_phrase")
                                 (:match-phrase-prefix "match-phrase-prefix")))
                 :fuzziness fuzziness
                 :zero-terms-query (when zero-terms-query
                                     (ecase zero-terms-query
                                       (:none "none")
                                       (:all "all")))
                 :cutoff-frequency cutoff-frequency
                 :boost boost))

(defclass <bool> (<boost-query> <filter> <minimum-should-match-query>)
  ((must :initarg :must
         :reader must)
   (must-not :initarg :must-not
               :reader must-not)
   (should :initarg :should
           :reader should)))

(defmethod encode-slots progn ((this <bool>))
  (with-object-element ("bool")
    (with-object ()
      (with-object-element* ("must" (must this))
        (encode-subquery (must this)))
      (with-object-element* ("must_not" (must-not this))
        (encode-subquery (must-not this)))
      (with-object-element* ("should" (should this))
        (encode-subquery (should this)))
      (encode-object-element* "minimum_should_match"
                              (minimum-should-match this))
      (encode-object-element* "boost" (boost this)))))

(defun bool (&key must must-not should minimum-should-match boost)
  (make-instance '<bool>
                 :must must
                 :must-not must-not
                 :should should
                 :minimum-should-match minimum-should-match
                 :boost boost))

(defclass <boosting> (<query>)
  ((positive :initarg :positive
             :reader positive)
   (negative :initarg :negative
             :reader negative)
   (negative-boost :initarg :negative-boost
                   :reader negative-boost)))

(defmethod encode-slots progn ((this <boosting>))
  (with-object-element ("boosting")
    (with-object ()
      (with-object-element ("positive")
        (encode-subquery (positive this)))
      (with-object-element ("negative")
        (encode-subquery (negative this)))
      (encode-object-element "negative_boost" (negative-boost this)))))

(defun boosting (positive negative negative-boost)
  (make-instance '<boosting>
                 :negative negative
                 :positive positive
                 :negative-boost negative-boost))

(defclass <filtered> (<query>)
  ((query :initarg :query
          :reader query)
   (filter :initarg :filter
           :reader filter)
   (strategy :initarg :strategy
             :reader strategy)))

(defmethod encode-slots progn ((this <filtered>))
  (with-object-element ("filtered")
    (with-object ()
      (with-object-element* ("query" (query this))
        (encode-object (query this)))
      (with-object-element ("filter")
        (encode-object (filter this)))
      (encode-object-element* "strategy" (strategy this)))))

(defun filtered (&key filter query strategy)
  (make-instance '<filtered>
                 :query query
                 :filter filter
                 :strategy (when strategy
                             (if (numberp strategy)
                                 (format nil "random_access_~D" strategy)
                                 (ecase strategy
                                   (:leap-frog-query-first
                                    "leap_frog_query_first")
                                   (:leap-frog-filter-first
                                    "leap_frog_filter_first")
                                   (:leap-frog
                                    "leap_frog")
                                   (:query-first
                                    "query_first")
                                   (:random-access-always
                                    "random_access_always"))))))

(defclass <terms> (<field-query>
                   <list-query>
                   <minimum-should-match-query>
                   <filter>)
  ())

(defmethod encode-slots progn ((this <terms>))
  (with-object-element ("terms")
    (with-object ()
      (with-object-element ((search-field this))
        (with-array ()
          (apply #'encode-array-elements (query-list this))))
      (encode-object-element* "minimum_should_match"
                              (minimum-should-match this)))))

(defun terms (query-list field &key minimum-should-match)
  (make-instance '<terms>
                 :search-field field
                 :query-list query-list
                 :minimum-should-match minimum-should-match))

(defclass <match-all> (<boost-query>) ())

(defmethod encode-slots progn ((this <match-all>))
  (with-object-element ("match_all")
    (with-object ()
      (encode-object-element* "boost" (boost this)))))

(defun match-all (&key boost)
  (make-instance '<match-all>
                 :boost boost))

(defclass <ids> (<query> <filter>)
  ((document-type :initarg :type
                  :reader document-type)
   (values :initarg :values
           :reader id-values)))

(defmethod encode-slots progn ((this <ids>))
  (with-object-element ("ids")
    (with-object ()
      (encode-object-element "values" (id-values this))
      (encode-object-element* "type" (document-type this)))))

(defun ids (type &rest values)
  (make-instance '<ids>
                 :type type
                 :values values))

(defclass <range> (<field-query> <boost-query> <filter>)
  ((gte :initarg :gte)
   (gt :initarg :gt)
   (lte :initarg :lte)
   (lt :initarg :lt)))

(defmethod encode-slots progn ((this <range>))
  (with-object-element ("range")
    (with-object ()
      (with-object-element ((search-field this))
        (with-object ()
          (encode-object-element* "gte" (slot-value this 'gte))
          (encode-object-element* "gt" (slot-value this 'gt))
          (encode-object-element* "lte" (slot-value this 'lte))
          (encode-object-element* "lt" (slot-value this 'lt))
          (unless (in-filter-position-p this)
            (encode-object-element* "boost" (boost this))))))))

(defun range (field &key gte gt lte lt boost)
  (ensure-mutually-exclusive gte gt)
  (ensure-mutually-exclusive lte lt)
  (make-instance '<range>
                 :search-field field
                 :boost boost
                 :gte gte
                 :gt gt
                 :lte lte
                 :lt lt))

(defclass <parent-child> (<filter> <score-mode-query>)
  ((query :initarg :query
          :reader subquery)
   (type :initarg :type
         :reader document-type)))

(defclass <has-child> (<parent-child>)
  ((min-children :initarg :min-children
                 :reader min-children)
   (max-children :initarg :max-children
                 :reader max-children)))

(defmethod encode-slots progn ((this <has-child>))
  (with-object-element ("has_child")
    (with-object ()
      (with-object-element ("query")
        (encode-object (subquery this)))
      (encode-object-element "type" (document-type this))
      (encode-object-element* "score_mode" (score-mode this))
      (encode-object-element* "min_children" (min-children this))
      (encode-object-element* "max_children" (max-children this)))))

(defun has-child (child-type query &key min-children max-children score-mode)
  (make-instance '<has-child>
                 :type child-type
                 :query query
                 :min-children min-children
                 :max-children max-children
                 :score-mode (when score-mode
                               (ecase score-mode
                                 (:max "max")
                                 (:sum "sum")
                                 (:avg "avg")
                                 (:none "none")))))

(defclass <has-parent> (<parent-child>) ())

(defmethod encode-slots progn ((this <has-parent>))
  (with-object-element ("has_parent")
    (with-object ()
      (with-object-element ("query")
        (encode-object (subquery this)))
      (encode-object-element "parent_type" (document-type this))
      (encode-object-element* "score_mode" (score-mode this)))))

(defun has-parent (parent-type query &key score-mode)
  (make-instance '<has-parent>
                 :type parent-type
                 :query query
                 :score-mode (when score-mode
                               (ecase score-mode
                                 (:score "score")
                                 (:none "none")))))

(defclass <prefix> (<string-query>
                    <field-query>
                    <boost-query>
                    <filter>) ())

(defmethod encode-slots progn ((this <prefix>))
  (with-object-element ("prefix")
    (with-object ()
      (with-object-element ((search-field this))
        (with-object ()
          (encode-object-element "value" (query-string this))
          (unless (in-filter-position-p this)
            (encode-object-element* "boost" (boost this))))))))

(defun prefix (query-string field &key boost)
  (make-instance '<prefix>
                 :query-string query-string
                 :search-field field
                 :boost boost))
