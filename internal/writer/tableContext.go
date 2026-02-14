package writer

import "reflect"

type TableContext struct {
	tableName          string
	columns            []columnWithDeclaration
	fields             []reflect.StructField
	insertStatement    string
	hasOnChangeColumns bool
	previousArgs       []any
}
