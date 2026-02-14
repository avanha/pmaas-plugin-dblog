package poller

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"pmaas.io/plugins/dblog/internal/common"
	"pmaas.io/plugins/dblog/internal/writer"
)

type fieldOptions struct {
	Mode      int
	Name      string
	MaxLength int
	Nullable  bool
	DataType  int
}

type tagPartHandlerFunc func(string, string, *fieldOptions) error

var nameValidator = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

var dataTypeMap = map[string]int{
	"int":        writer.DataTypeInt,
	"bigint":     writer.DataTypeBigInt,
	"decimal":    writer.DataTypeDecimal,
	"bigdecimal": writer.DataTypeBigDecimal,
	"varchar":    writer.DataTypeVarChar,
	"timestamp":  writer.DataTypeTimestamp,
}

var tagHandlers = map[string]tagPartHandlerFunc{
	"always": func(tagName string, tagValue string, fo *fieldOptions) error {
		if fo.Mode != 0 {
			return fmt.Errorf("duplicate mode tags")
		}

		if tagValue != "" {
			return fmt.Errorf("always tag does not support a value")
		}

		fo.Mode = common.FieldModeAlways

		return nil
	},
	"onchange": func(tagName string, tagValue string, fo *fieldOptions) error {
		if fo.Mode != 0 {
			return fmt.Errorf("duplicate mode tags")
		}

		if tagValue != "" {
			return fmt.Errorf("onchange tag does not support a value")
		}

		fo.Mode = common.FieldModeOnChange
		fo.Nullable = true

		return nil
	},
	"dataType": func(tagName string, tagValue string, fo *fieldOptions) error {
		if fo.DataType != 0 {
			return fmt.Errorf("duplicate dataType tags")
		}

		if tagValue == "" {
			return fmt.Errorf("blank dataType tag value")
		}

		dataType, ok := dataTypeMap[tagValue]

		if !ok {
			return fmt.Errorf("invalid dataType tag value: \"%s\"", tagValue)
		}

		fo.DataType = dataType

		return nil
	},
	"maxLength": func(tagName string, tagValue string, fo *fieldOptions) error {
		if fo.MaxLength != 0 {
			return fmt.Errorf("duplicate maxLength tags")
		}

		if tagValue == "" {
			return fmt.Errorf("blank maxLength tag value")
		}

		value, err := strconv.Atoi(tagValue)

		if err != nil {
			return fmt.Errorf("invalid maxLength tag value: \"%s\" is not an integer", tagValue)
		}

		fo.MaxLength = value

		return nil
	},
	"nullable": func(tagName string, tagValue string, fo *fieldOptions) error {
		if tagValue != "" {
			return fmt.Errorf("nullable tag does not support a value")
		}

		fo.Nullable = true

		return nil
	},
	"name": func(tagName string, tagValue string, fo *fieldOptions) error {
		if tagValue == "" {
			return fmt.Errorf("blank name tag value")
		}

		if !nameValidator.MatchString(tagValue) {
			return fmt.Errorf("invalid name tag value: \"%s\". "+
				" names must be composed of upper or lower-case letters, digits and under scores", tagValue)
		}

		fo.Name = tagValue

		return nil
	},
}

func parseFieldTag(tag string) (fieldOptions, error) {
	result := fieldOptions{}
	parts := strings.Split(tag, ",")

	for _, part := range parts {
		keyAndValue := strings.Split(part, "=")
		keyAndValueLength := len(keyAndValue)

		if keyAndValueLength == 0 || keyAndValueLength > 2 {
			return result, fmt.Errorf("invalid field tag content: %s", part)
		}

		key := keyAndValue[0]
		value := ""

		if keyAndValueLength == 2 {
			value = keyAndValue[1]
		}

		handler, ok := tagHandlers[key]

		if !ok {
			return result, fmt.Errorf("unknown field option: %s", key)
		}

		err := handler(key, value, &result)

		if err != nil {
			return result, err
		}
	}

	return result, nil
}
