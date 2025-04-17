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

	"github.com/rancher/steve/pkg/stores/sqlpartition/selection"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
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
		"metadata.labels[k8s.io/meta-stuff] => [management.cattle.io/v3][tokens][id][metadata.state.name] = active",
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
		`x="no double quotes allowed"`,
		`x='no single quotes allowed'`,
		"name => [management.cattle.io/v3][tokens][id][metadata.state.name] = active",
		"metadata.annotations[blah] => [management.cattle.io/v3][tokens][id][metadata.state.name] = active",
		"metadata.labels[k8s.io/meta-stuff] => not-bracketed = active",
		"metadata.labels[k8s.io/meta-stuff] => [not][enough][accessors] = active",
		"metadata.labels[k8s.io/meta-stuff] => [too][many][accessors][by][1] = active",
		"metadata.labels[k8s.io/meta-stuff] => [missing][an][operator][end-of-string]",
		"metadata.labels[k8s.io/meta-stuff] => [missing][an][operator][no-following-operator] no-operator",
		"metadata.labels[k8s.io/meta-stuff] => [missing][a][post-operator][value] >",
		"metadata.labels[not/followed/by/accessor] => = active",
	}
	for _, test := range testGoodStrings {
		_, err := Parse(test, "filter")
		if err != nil {
			t.Errorf("%v: error %v (%#v)\n", test, err, err)
		}
	}
	for _, test := range testBadStrings {
		_, err := Parse(test, "filter")
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
		{`'sq string''`, ErrorToken},
		{`"dq string"`, ErrorToken},
		{"~", PartialEqualsToken},
		{"!~", NotPartialEqualsToken},
		{"||", ErrorToken},
		{"=>", IndirectAccessToken},
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
		{"metadata.labels[k8s.io/meta-stuff] => [management.cattle.io/v3][tokens][id][metadata.state.name] = active",
			[]Token{IdentifierToken, IndirectAccessToken, IdentifierToken, EqualsToken, IdentifierToken},
		},
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
		{"key gt 3", []Token{IdentifierToken, GreaterThanToken, IdentifierToken, EndOfStringToken}},
		{"key lt 4", []Token{IdentifierToken, LessThanToken, IdentifierToken, EndOfStringToken}},
		{`key = multi-word-string`, []Token{IdentifierToken, EqualsToken, QuotedStringToken, EndOfStringToken}},

		{"metadata.labels[k8s.io/meta-stuff] => [management.cattle.io/v3][tokens][id][metadata.state.name] = active",
			[]Token{IdentifierToken, IndirectAccessToken, IdentifierToken, EqualsToken, IdentifierToken, EndOfStringToken},
		},
	}
	for _, v := range testcases {
		p := &Parser{l: &Lexer{s: v.s, pos: 0}, position: 0}
		p.scan()
		if len(p.scannedItems) != len(v.t) {
			t.Errorf("Expected %d items for test %s, found %d", len(v.t), v.s, len(p.scannedItems))
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
		{"~", nil},
		{">", nil},
		{"<", nil},
		{"lt", nil},
		{"gt", nil},
		{"notin", nil},
		{"!=", nil},
		{"!~", nil},
		{"=>", nil},
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
		WantErr string
	}{
		{
			Key:     "x1",
			Op:      selection.In,
			WantErr: "values: Invalid value: []string{}: for 'in', 'notin' operators, values set can't be empty",
		},
		{
			Key:     "x2",
			Op:      selection.NotIn,
			Vals:    sets.NewString(),
			WantErr: "values: Invalid value: []string{}: for 'in', 'notin' operators, values set can't be empty",
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
			Key:     "x5",
			Op:      selection.Equals,
			Vals:    sets.NewString("foo", "bar"),
			WantErr: "values: Invalid value: []string{\"bar\", \"foo\"}: exact-match compatibility requires one single value",
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
			Key:     "x8",
			Op:      selection.Exists,
			Vals:    sets.NewString("foo"),
			WantErr: `values: Invalid value: []string{"foo"}: values set must be empty for exists and does not exist`,
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
			Key:     "x13",
			Op:      selection.GreaterThan,
			WantErr: "values: Invalid value: []string{}: for 'Gt', 'Lt' operators, exactly one value is required",
		},
		{
			Key:     "x14",
			Op:      selection.GreaterThan,
			Vals:    sets.NewString("bar"),
			WantErr: `values[0]: Invalid value: "bar": for 'Gt', 'Lt' operators, the value must be an integer`,
		},
		{
			Key:     "x15",
			Op:      selection.LessThan,
			Vals:    sets.NewString("bar"),
			WantErr: `values[0]: Invalid value: "bar": for 'Gt', 'Lt' operators, the value must be an integer`,
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
			Key:     "x18",
			Op:      "unsupportedOp",
			WantErr: `operator: Unsupported value: "unsupportedOp": supported values: "in", "notin", "=", "==", "!=", "~", "!~", "gt", "lt", "=>", "exists", "!"`,
		},
	}
	for _, rc := range requirementConstructorTests {
		_, err := NewRequirement(rc.Key, rc.Op, rc.Vals.List())
		if rc.WantErr != "" {
			assert.NotNil(t, err)
			if err != nil {
				assert.Equal(t, rc.WantErr, err.Error())
			}
		} else {
			assert.Nil(t, err)
		}
		_, err = NewIndirectRequirement(rc.Key, []string{"herb", "job", "nice", "reading"}, &rc.Op, rc.Vals.List())
		if rc.WantErr != "" {
			assert.NotNil(t, err)
			if err != nil {
				assert.Equal(t, rc.WantErr, err.Error())
			}
		} else {
			assert.Nil(t, err)
		}
	}
}
