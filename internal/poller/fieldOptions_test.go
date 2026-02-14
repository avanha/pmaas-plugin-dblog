package poller

import (
	"testing"

	"github.com/avanha/pmaas-plugin-dblog/internal/writer"
)

func TestFieldOptions_parseFieldTag_succeeds(t *testing.T) {
	result, err := parseFieldTag("always,name=customized_name,maxLength=255,nullable,dataType=varchar")

	if err != nil {
		t.Fatalf("parse failed: %s", err)
	}

	expectedFieldOptions := fieldOptions{
		Mode:      FieldModeAlways,
		Name:      "customized_name",
		MaxLength: 255,
		Nullable:  true,
		DataType:  writer.DataTypeVarChar,
	}

	if result != expectedFieldOptions {
		t.Fatalf("expected %+v, got %+v", expectedFieldOptions, result)
	}
}

func TestFieldOptions_parseFieldTag_modeOnChange_setModeAndNullable(t *testing.T) {
	result, err := parseFieldTag("onchange")

	if err != nil {
		t.Fatalf("parse failed: %s", err)
	}

	expectedFieldOptions := fieldOptions{
		Mode:     FieldModeOnChange,
		Nullable: true,
	}

	if result != expectedFieldOptions {
		t.Fatalf("expected %+v, got %+v", expectedFieldOptions, result)
	}
}

func TestFieldOptions_parseFieldTag_multipleModes_failsWithError(t *testing.T) {
	_, err := parseFieldTag("always,onchange")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "duplicate mode tags"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}

func TestFieldOptions_parseFieldTag_multipleEquals_failsWithError(t *testing.T) {
	_, err := parseFieldTag("key=value1=value2")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "invalid field tag content: key=value1=value2"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}

func TestFieldOptions_parseFieldTag_invalidTag_failsWithError(t *testing.T) {
	_, err := parseFieldTag("invalidTag")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "unknown field option: invalidTag"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}

func TestFieldOptions_parseFieldTag_invalidMaxLength_failsWithError(t *testing.T) {
	_, err := parseFieldTag("maxLength=invalid")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "invalid maxLength tag value: \"invalid\" is not an integer"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}

func TestFieldOptions_parseFieldTag_invalidName_failsWithError(t *testing.T) {
	_, err := parseFieldTag("name=Invalid Name")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "invalid name tag value: \"Invalid Name\".  names must be composed of upper or lower-case letters, digits and under scores"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}

func TestFieldOptions_parseFieldTag_invalidNullable_failsWithError(t *testing.T) {
	_, err := parseFieldTag("nullable=true")

	if err == nil {
		t.Fatalf("parse expected to fail, but was successful")
	}

	expectedError := "nullable tag does not support a value"

	if err.Error() != expectedError {
		t.Fatalf("expected error \"%s\", got \"%s\"", expectedError, err.Error())
	}
}
