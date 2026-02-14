package writer

import "reflect"

const DataTypeInt = 1
const DataTypeBigInt = 2
const DataTypeDecimal = 3
const DataTypeBigDecimal = 4
const DataTypeVarChar = 5
const DataTypeTimestamp = 6

type Column struct {
	Name      string
	DataType  int
	MaxLength int
	Nullable  bool
	Mode      int
	Field     reflect.StructField
}
