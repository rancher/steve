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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/rancher/steve/pkg/stores/sqlpartition/selection"
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
		"close ~ value",
		"notclose !~ value",
		"x>1",
		"x>1,z<5",
		"x gt 1,z lt 5",
		`y == def`,
		"metadata.labels.im-here",
		"!metadata.labels.im-not-here",
		"metadata.labels[im.here]",
		"!metadata.labels[im.not.here]",
		"metadata.labels[k8s.io/meta-stuff] ~ has-dashes_underscores.dots.only",
		`metadata.labels[k8s.io/meta-stuff] ~ "m!a@t#c$h%e^v&e*r(y)t-_i=n+g)t{o[$]c}o]m|m\\a:;'<.>"`,
		`x="double quotes ok"`,
		`x='single quotes ok'`,
		`x="double quotes with \\ and \" ok"`,
		`x='single quotes with \\ and \' ok'`,
		`x contains "app=nginx"`,
		`x notcontains "app=nginx"`,
	}
	testBadStrings := []string{
		"!no-label-absence-test",
		"no-label-presence-test",
		"x=a||y=b",
		"x==a==b",
		"!x=a",
		"x<a",
		"x=",
		"x= ",
		"x=,z= ",
		"x= ,z= ",
		"x ~",
		"x !~",
		"~ val",
		"!~ val",
		"= val",
		"== val",
		"metadata.labels-im.here",
		"metadata.labels[missing/close-bracket",
		"!metadata.labels(im.not.here)",
		"x contains",
		"x contains a,b",
		"x notcontains a,b",
	}
	for _, test := range testGoodStrings {
		if test == `x notcontains "app=nginx"` {
			fmt.Println("stop here")
		}
		_, err := Parse(test)
		if err != nil {
			t.Errorf("%v: error %v (%#v)\n", test, err, err)
		}
	}
	for _, test := range testBadStrings {
		if test == "x notcontains a,b" {
			fmt.Println("stop here")
		}
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
		{"contains", ContainsToken},
		{"notcontains", NotContainsToken},
		{"=", EqualsToken},
		{"==", DoubleEqualsToken},
		{">", GreaterThanToken},
		{"<", LessThanToken},
		//Note that Lex returns the longest valid token found
		{"!", DoesNotExistToken},
		{"!=", NotEqualsToken},
		{"(", OpenParToken},
		{")", ClosedParToken},
		{`'sq string'`, QuotedStringToken},
		{`"dq string"`, QuotedStringToken},
		{"~", PartialEqualsToken},
		{"!~", NotPartialEqualsToken},
		{"||", ErrorToken},
		{`"double-quoted string"`, QuotedStringToken},
		{`'single-quoted string'`, QuotedStringToken},
	}
	for _, v := range testcases {
		l := &Lexer{s: v.s, pos: 0}
		token, lit := l.Lex()
		if token != v.t {
			t.Errorf("Got %d it should be %d for '%s'", token, v.t, v.s)
		}
		if v.t != ErrorToken {
			exp := v.s
			if v.t == QuotedStringToken {
				exp = exp[1 : len(exp)-1]
			}
			if lit != exp {
				t.Errorf("Got '%s' it should be '%s'", lit, exp)
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
		{"key gt 3", []Token{IdentifierToken, IdentifierToken, IdentifierToken}},
		{"key lt 4", []Token{IdentifierToken, IdentifierToken, IdentifierToken}},
		{`key = 'sqs'`, []Token{IdentifierToken, EqualsToken, QuotedStringToken}},
		{`key = "dqs"`, []Token{IdentifierToken, EqualsToken, QuotedStringToken}},
		{"key=value", []Token{IdentifierToken, EqualsToken, IdentifierToken}},
		{"key == value", []Token{IdentifierToken, DoubleEqualsToken, IdentifierToken}},
		{"key ~ value", []Token{IdentifierToken, PartialEqualsToken, IdentifierToken}},
		{"key~ value", []Token{IdentifierToken, PartialEqualsToken, IdentifierToken}},
		{"key ~value", []Token{IdentifierToken, PartialEqualsToken, IdentifierToken}},
		{"key~value", []Token{IdentifierToken, PartialEqualsToken, IdentifierToken}},
		{"key !~ value", []Token{IdentifierToken, NotPartialEqualsToken, IdentifierToken}},
		{"key!~ value", []Token{IdentifierToken, NotPartialEqualsToken, IdentifierToken}},
		{"key !~value", []Token{IdentifierToken, NotPartialEqualsToken, IdentifierToken}},
		{"key!~value", []Token{IdentifierToken, NotPartialEqualsToken, IdentifierToken}},
		{`ip(status.podIP)`, []Token{IdentifierToken, OpenParToken, IdentifierToken, ClosedParToken}},
		{`x contains "app=moose"`, []Token{IdentifierToken, ContainsToken, QuotedStringToken}},
		{`x notcontains "app=moose"`, []Token{IdentifierToken, NotContainsToken, QuotedStringToken}},
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
		{"key gt 3", []Token{IdentifierToken, IdentifierToken, IdentifierToken, EndOfStringToken}},
		{"key lt 4", []Token{IdentifierToken, IdentifierToken, IdentifierToken, EndOfStringToken}},
		{`key = "multi-word-string"`, []Token{IdentifierToken, EqualsToken, QuotedStringToken, EndOfStringToken}},
		{`ip(status.podIP)`, []Token{IdentifierToken, OpenParToken, IdentifierToken, ClosedParToken, EndOfStringToken}},
		{`metadata.labels.dog contains "cat=meow"`, []Token{IdentifierToken, ContainsToken, QuotedStringToken, EndOfStringToken}},
		{`metadata.labels.dog notcontains "pig=oink"`, []Token{IdentifierToken, NotContainsToken, QuotedStringToken, EndOfStringToken}},
	}
	for _, v := range testcases {
		p := &Parser{l: &Lexer{s: v.s, pos: 0}, position: 0}
		p.scan()
		if len(p.scannedItems) != len(v.t) {
			t.Errorf("Expected %d items for test %s, found %d", len(v.t), v.s, len(p.scannedItems))
		}
		for i, entry := range v.t {
			token, _ := p.consume(KeyAndOperator)
			if token == EndOfStringToken {
				if i != len(v.t)-1 {
					t.Errorf("Expected end of string token at position %d for test '%s', but length is %d", i, v.s, len(v.t))
				}
				break
			}
			if token != entry {
				t.Errorf("Expected token %v at position %d for test '%s', but got %v", entry, i, v.s, token)
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
		{"~", nil},
		{">", nil},
		{"<", nil},
		{"lt", nil},
		{"gt", nil},
		{"notin", nil},
		{"!=", nil},
		{"!~", nil},
		{"contains", nil},
		{"notcontains", nil},
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
		},
		{
			Key:  "x16",
			Op:   selection.Equals,
			Vals: sets.NewString(strings.Repeat("a", 254)),
		},
		{
			Key:  "x17",
			Op:   selection.Equals,
			Vals: sets.NewString("a b"),
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
