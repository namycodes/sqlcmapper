package sqlcmapper

import (
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

/////////////////////
// Postgres helpers
/////////////////////

func PgUUIDToString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return uuid.UUID(id.Bytes).String()
}

func PgTextToStringPtr(txt pgtype.Text) *string {
	if !txt.Valid {
		return nil
	}
	return &txt.String
}

func PgFloat8ToFloat64Ptr(f pgtype.Float8) *float64 {
	if !f.Valid {
		return nil
	}
	return &f.Float64
}

func PgInt4ToInt32Ptr(i pgtype.Int4) *int32 {
	if !i.Valid {
		return nil
	}
	return &i.Int32
}

func PgBoolToBoolPtr(b pgtype.Bool) *bool {
	if !b.Valid {
		return nil
	}
	return &b.Bool
}

func PgTimestamptzToString(ts pgtype.Timestamptz) string {
	if !ts.Valid {
		return ""
	}
	return ts.Time.Format(time.RFC3339)
}

/////////////////////
// GenericMapper
/////////////////////

type GenericMapper[From any, To any] struct {
	mapFunc func(From) To
}

func NewGenericMapper[From any, To any](fn func(From) To) *GenericMapper[From, To] {
	return &GenericMapper[From, To]{mapFunc: fn}
}

func (m *GenericMapper[From, To]) Map(f From) To {
	return m.mapFunc(f)
}

func (m *GenericMapper[From, To]) MapSlice(fs []From) []To {
	out := make([]To, len(fs))
	for i, f := range fs {
		out[i] = m.mapFunc(f)
	}
	return out
}

/////////////////////
// Reflection-based AutoMapWithTags
/////////////////////

func AutoMapWithTags[DB any, Model any](dbStruct DB) (Model, error) {
	res, err := autoMapWithTagsInterface(dbStruct, reflect.TypeOf((*Model)(nil)).Elem())
	if err != nil {
		return *new(Model), err
	}
	return res.Interface().(Model), nil
}

func AutoMapSliceWithTags[DB any, Model any](dbSlice []DB) ([]Model, error) {
	out := make([]Model, len(dbSlice))
	for i, dbItem := range dbSlice {
		mapped, err := AutoMapWithTags[DB, Model](dbItem)
		if err != nil {
			return nil, err
		}
		out[i] = mapped
	}
	return out, nil
}

func autoMapWithTagsInterface(dbStruct interface{}, modelType reflect.Type) (reflect.Value, error) {
	dbVal := reflect.ValueOf(dbStruct)
	if dbVal.Kind() == reflect.Ptr {
		dbVal = dbVal.Elem()
	}

	modelVal := reflect.New(modelType).Elem()

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)

		dbTag := fieldType.Tag.Get("db")
		if dbTag == "" {
			dbTag = fieldType.Name
		}

		dbField := dbVal.FieldByNameFunc(func(name string) bool {
			return name == dbTag || toSnakeCase(name) == dbTag
		})

		if !dbField.IsValid() {
			continue
		}

		switch dbField.Interface().(type) {
		case pgtype.UUID:
			if field.Kind() == reflect.String {
				field.SetString(PgUUIDToString(dbField.Interface().(pgtype.UUID)))
			}
		case pgtype.Text:
			if field.Kind() == reflect.Ptr && field.Type().Elem().Kind() == reflect.String {
				field.Set(reflect.ValueOf(PgTextToStringPtr(dbField.Interface().(pgtype.Text))))
			}
		case pgtype.Float8:
			if field.Kind() == reflect.Ptr && field.Type().Elem().Kind() == reflect.Float64 {
				field.Set(reflect.ValueOf(PgFloat8ToFloat64Ptr(dbField.Interface().(pgtype.Float8))))
			}
		case pgtype.Int4:
			if field.Kind() == reflect.Ptr && field.Type().Elem().Kind() == reflect.Int32 {
				field.Set(reflect.ValueOf(PgInt4ToInt32Ptr(dbField.Interface().(pgtype.Int4))))
			}
		case pgtype.Bool:
			if field.Kind() == reflect.Ptr && field.Type().Elem().Kind() == reflect.Bool {
				field.Set(reflect.ValueOf(PgBoolToBoolPtr(dbField.Interface().(pgtype.Bool))))
			}
		case pgtype.Timestamptz:
			if field.Kind() == reflect.String {
				field.SetString(PgTimestamptzToString(dbField.Interface().(pgtype.Timestamptz)))
			}
		default:
			if field.Kind() == reflect.Struct && dbField.Kind() == reflect.Struct {
				mappedField, err := autoMapWithTagsInterface(dbField.Interface(), field.Type())
				if err != nil {
					return modelVal, err
				}
				field.Set(mappedField)
				continue
			}
			if field.Kind() == reflect.Slice && dbField.Kind() == reflect.Slice {
				sliceType := field.Type().Elem()
				mappedSlice := reflect.MakeSlice(field.Type(), dbField.Len(), dbField.Len())
				for j := 0; j < dbField.Len(); j++ {
					mappedElem, err := autoMapWithTagsInterface(dbField.Index(j).Interface(), sliceType)
					if err != nil {
						return modelVal, err
					}
					mappedSlice.Index(j).Set(mappedElem)
				}
				field.Set(mappedSlice)
				continue
			}
			if dbField.Type().AssignableTo(field.Type()) {
				field.Set(dbField)
			}
		}
	}

	return modelVal, nil
}

// helper: CamelCase -> snake_case
func toSnakeCase(s string) string {
	out := ""
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				out += "_"
			}
			out += string(c + ('a' - 'A'))
		} else {
			out += string(c)
		}
	}
	return out
}
