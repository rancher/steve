/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
This file is derived from
https://github.com/kubernetes/apimachinery/blob/master/pkg/labels/selector_test.go
*/

package queryparser

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	ignoreDetail = cmpopts.IgnoreFields(field.Error{}, "Detail")
)

func TestSelectorParse(t *testing.T) {
	testGoodStrings := []string{
		"x=a,y=b,z=c",
		"",
		"x!=a,y=b",
		"x=",
		"x= ",
		"x=,z= ",
		"x= ,z= ",
		"!x",
		"x>1",
		"x>1,z<5",
		`x == "abc"`,
		`y == 'def'`,
	}
	testBadStrings := []string{
		"x=a||y=b",
		"x==a==b",
		"!x=a",
		"x<a",
		`x == "abc`,
		`y == 'def`,
		`z == 'ghi\`,
	}
	for _, test := range testGoodStrings {
		_, err := Parse(test)
		if err != nil {
			t.Errorf("%v: error %v (%#v)\n", test, err, err)
		}
	}
	for _, test := range testBadStrings {
		_, err := Parse(test)
		if err == nil {
			t.Errorf("%v: did not get expected error\n", test)
		}
	}
}

func TestLexer(t *testing.T) {
	testcases := []struct {
		s string
		t Token
	}{
		{"", EndOfStringToken},
		{",", CommaToken},
		{"notin", NotInToken},
		{"in", InToken},
		{"=", EqualsToken},
		{"==", DoubleEqualsToken},
		{">", GreaterThanToken},
		{"<", LessThanToken},
		//Note that Lex returns the longest valid token found
		{"!", DoesNotExistToken},
		{"!=", NotEqualsToken},
		{"(", OpenParToken},
		{")", ClosedParToken},
		//Non-"special" characters are considered part of an identifier
		{"~", IdentifierToken},
		{"||", IdentifierToken},
		{`"unclosed dq string`, ErrorToken},
		{`'unclosed sq string`, ErrorToken},
		{`'unclosed sq string on an escape \`, ErrorToken},
	}
	for _, v := range testcases {
		l := &Lexer{s: v.s, pos: 0}
		token, lit := l.Lex()
		if token != v.t {
			t.Errorf("Got %d it should be %d for '%s'", token, v.t, v.s)
		}
		if v.t != ErrorToken && lit != v.s {
			t.Errorf("Got '%s' it should be '%s'", lit, v.s)
		}
	}
}
func TestQuotedStringLexer(t *testing.T) {
	testcases := []struct {
		s string
		t Token
	}{
		{`"abc"`, QuotedStringToken},
		{`'def'`, QuotedStringToken},
		{`"abc, bs:\\, dq:\", sq:\', x:\x"`, QuotedStringToken},
		{`'def, bs:\\, dq:\", sq:\', x:\x'`, QuotedStringToken},
	}
	rx := regexp.MustCompile(`\\(.)`)
	for _, v := range testcases {
		l := &Lexer{s: v.s, pos: 0}
		token, lit := l.Lex()
		if token != v.t {
			t.Errorf("Got %d it should be %d for '%s'", token, v.t, v.s)
		}
		if v.t != ErrorToken {
			//expectedLit := v.s[1 : len(v.s)-1]
			expectedLit := v.s
			expectedLit = rx.ReplaceAllString(expectedLit, "$1")
			if lit != expectedLit {
				t.Errorf("Got '%s' it should be '%s'", lit, expectedLit)
			}
		}
	}
}

func min(l, r int) (m int) {
	m = r
	if l < r {
		m = l
	}
	return m
}

func TestLexerSequence(t *testing.T) {
	testcases := []struct {
		s string
		t []Token
	}{
		{"key in ( value )", []Token{IdentifierToken, InToken, OpenParToken, IdentifierToken, ClosedParToken}},
		{"key notin ( value )", []Token{IdentifierToken, NotInToken, OpenParToken, IdentifierToken, ClosedParToken}},
		{"key in ( value1, value2 )", []Token{IdentifierToken, InToken, OpenParToken, IdentifierToken, CommaToken, IdentifierToken, ClosedParToken}},
		{"key", []Token{IdentifierToken}},
		{"!key", []Token{DoesNotExistToken, IdentifierToken}},
		{"()", []Token{OpenParToken, ClosedParToken}},
		{"x in (),y", []Token{IdentifierToken, InToken, OpenParToken, ClosedParToken, CommaToken, IdentifierToken}},
		{"== != (), = notin", []Token{DoubleEqualsToken, NotEqualsToken, OpenParToken, ClosedParToken, CommaToken, EqualsToken, NotInToken}},
		{"key>2", []Token{IdentifierToken, GreaterThanToken, IdentifierToken}},
		{"key<1", []Token{IdentifierToken, LessThanToken, IdentifierToken}},
		{"key=value", []Token{IdentifierToken, EqualsToken, IdentifierToken}},
		{"key == value", []Token{IdentifierToken, DoubleEqualsToken, IdentifierToken}},
		{`"abc"`, []Token{QuotedStringToken}},
		{"'def'", []Token{QuotedStringToken}},
	}
	for _, v := range testcases {
		var tokens []Token
		l := &Lexer{s: v.s, pos: 0}
		for {
			token, _ := l.Lex()
			if token == EndOfStringToken {
				break
			}
			tokens = append(tokens, token)
		}
		if len(tokens) != len(v.t) {
			t.Errorf("Bad number of tokens for '%s': got %d, wanted %d (got %v)", v.s, len(tokens), len(v.t), tokens)
		}
		for i := 0; i < min(len(tokens), len(v.t)); i++ {
			if tokens[i] != v.t[i] {
				t.Errorf("Test '%s': Mismatching in token type found '%v' it should be '%v'", v.s, tokens[i], v.t[i])
			}
		}
	}
}
func TestParserLookahead(t *testing.T) {
	testcases := []struct {
		s string
		t []Token
	}{
		{"key in ( value )", []Token{IdentifierToken, InToken, OpenParToken, IdentifierToken, ClosedParToken, EndOfStringToken}},
		{"key notin ( value )", []Token{IdentifierToken, NotInToken, OpenParToken, IdentifierToken, ClosedParToken, EndOfStringToken}},
		{"key in ( value1, value2 )", []Token{IdentifierToken, InToken, OpenParToken, IdentifierToken, CommaToken, IdentifierToken, ClosedParToken, EndOfStringToken}},
		{"key", []Token{IdentifierToken, EndOfStringToken}},
		{"!key", []Token{DoesNotExistToken, IdentifierToken, EndOfStringToken}},
		{"()", []Token{OpenParToken, ClosedParToken, EndOfStringToken}},
		{"", []Token{EndOfStringToken}},
		{"x in (),y", []Token{IdentifierToken, InToken, OpenParToken, ClosedParToken, CommaToken, IdentifierToken, EndOfStringToken}},
		{"== != (), = notin", []Token{DoubleEqualsToken, NotEqualsToken, OpenParToken, ClosedParToken, CommaToken, EqualsToken, NotInToken, EndOfStringToken}},
		{"key>2", []Token{IdentifierToken, GreaterThanToken, IdentifierToken, EndOfStringToken}},
		{"key<1", []Token{IdentifierToken, LessThanToken, IdentifierToken, EndOfStringToken}},
		{`key == "dq string"`, []Token{IdentifierToken, DoubleEqualsToken, QuotedStringToken, EndOfStringToken}},
		{`key = 'sq string'`, []Token{IdentifierToken, EqualsToken, QuotedStringToken, EndOfStringToken}},
	}
	for _, v := range testcases {
		p := &Parser{l: &Lexer{s: v.s, pos: 0}, position: 0}
		p.scan()
		if len(p.scannedItems) != len(v.t) {
			t.Errorf("Expected %d items found %d", len(v.t), len(p.scannedItems))
		}
		for {
			token, lit := p.lookahead(KeyAndOperator)

			token2, lit2 := p.consume(KeyAndOperator)
			if token == EndOfStringToken {
				break
			}
			if token != token2 || lit != lit2 {
				t.Errorf("Bad values")
			}
		}
	}
}

func TestParseOperator(t *testing.T) {
	testcases := []struct {
		token         string
		expectedError error
	}{
		{"in", nil},
		{"=", nil},
		{"==", nil},
		{">", nil},
		{"<", nil},
		{"notin", nil},
		{"!=", nil},
		{"!", fmt.Errorf("found '%s', expected: %v", selection.DoesNotExist, strings.Join(binaryOperators, ", "))},
		{"exists", fmt.Errorf("found '%s', expected: %v", selection.Exists, strings.Join(binaryOperators, ", "))},
		{"(", fmt.Errorf("found '%s', expected: %v", "(", strings.Join(binaryOperators, ", "))},
	}
	for _, testcase := range testcases {
		p := &Parser{l: &Lexer{s: testcase.token, pos: 0}, position: 0}
		p.scan()

		_, err := p.parseOperator()
		if ok := reflect.DeepEqual(testcase.expectedError, err); !ok {
			t.Errorf("\nexpect err [%v], \nactual err [%v]", testcase.expectedError, err)
		}
	}
}

// Some error fields are commented out here because this fork no longer
// enforces k8s label expression lexical and length restrictions
func TestRequirementConstructor(t *testing.T) {
	requirementConstructorTests := []struct {
		Key     string
		Op      selection.Operator
		Vals    sets.String
		WantErr field.ErrorList
	}{
		{
			Key: "x1",
			Op:  selection.In,
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values",
					BadValue: []string{},
				},
			},
		},
		{
			Key:  "x2",
			Op:   selection.NotIn,
			Vals: sets.NewString(),
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values",
					BadValue: []string{},
				},
			},
		},
		{
			Key:  "x3",
			Op:   selection.In,
			Vals: sets.NewString("foo"),
		},
		{
			Key:  "x4",
			Op:   selection.NotIn,
			Vals: sets.NewString("foo"),
		},
		{
			Key:  "x5",
			Op:   selection.Equals,
			Vals: sets.NewString("foo", "bar"),
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values",
					BadValue: []string{"bar", "foo"},
				},
			},
		},
		{
			Key: "x6",
			Op:  selection.Exists,
		},
		{
			Key: "x7",
			Op:  selection.DoesNotExist,
		},
		{
			Key:  "x8",
			Op:   selection.Exists,
			Vals: sets.NewString("foo"),
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values",
					BadValue: []string{"foo"},
				},
			},
		},
		{
			Key:  "x9",
			Op:   selection.In,
			Vals: sets.NewString("bar"),
		},
		{
			Key:  "x10",
			Op:   selection.In,
			Vals: sets.NewString("bar"),
		},
		{
			Key:  "x11",
			Op:   selection.GreaterThan,
			Vals: sets.NewString("1"),
		},
		{
			Key:  "x12",
			Op:   selection.LessThan,
			Vals: sets.NewString("6"),
		},
		{
			Key: "x13",
			Op:  selection.GreaterThan,
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values",
					BadValue: []string{},
				},
			},
		},
		{
			Key:  "x14",
			Op:   selection.GreaterThan,
			Vals: sets.NewString("bar"),
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values[0]",
					BadValue: "bar",
				},
			},
		},
		{
			Key:  "x15",
			Op:   selection.LessThan,
			Vals: sets.NewString("bar"),
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeInvalid,
					Field:    "values[0]",
					BadValue: "bar",
				},
			},
		},
		{
			Key: strings.Repeat("a", 254), //breaks DNS rule that len(key) <= 253
			Op:  selection.Exists,
			//WantErr: field.ErrorList{
			//	&field.Error{
			//		Type:     field.ErrorTypeInvalid,
			//		Field:    "key",
			//		BadValue: strings.Repeat("a", 254),
			//	},
			//},
		},
		{
			Key:  "x16",
			Op:   selection.Equals,
			Vals: sets.NewString(strings.Repeat("a", 254)),
			//WantErr: field.ErrorList{
			//	&field.Error{
			//		Type:     field.ErrorTypeInvalid,
			//		Field:    "values[0][x16]",
			//		BadValue: strings.Repeat("a", 254),
			//	},
			//},
		},
		{
			Key:  "x17",
			Op:   selection.Equals,
			Vals: sets.NewString("a b"),
			//WantErr: field.ErrorList{
			//	&field.Error{
			//		Type:     field.ErrorTypeInvalid,
			//		Field:    "values[0][x17]",
			//		BadValue: "a b",
			//	},
			//},
		},
		{
			Key: "x18",
			Op:  "unsupportedOp",
			WantErr: field.ErrorList{
				&field.Error{
					Type:     field.ErrorTypeNotSupported,
					Field:    "operator",
					BadValue: selection.Operator("unsupportedOp"),
				},
			},
		},
	}
	for _, rc := range requirementConstructorTests {
		_, err := NewRequirement(rc.Key, rc.Op, rc.Vals.List())
		if diff := cmp.Diff(rc.WantErr.ToAggregate(), err, ignoreDetail); diff != "" {
			t.Errorf("NewRequirement test %v returned unexpected error (-want,+got):\n%s", rc.Key, diff)
		}
	}
}
