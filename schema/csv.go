package schema

import (
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/common"
	"github.com/sabey/parquet-go/parquet"
)

//Create a schema handler from CSV metadata
func NewSchemaHandlerFromMetadata(mds []string) (*SchemaHandler, error) {
	schemaList := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "Parquet_go_root"
	rootNumChildren := int32(len(mds))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType_REQUIRED
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	rootInfo := common.NewTag()
	rootInfo.InName = "Parquet_go_root"
	rootInfo.ExName = "parquet_go_root"
	rootInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	infos = append(infos, rootInfo)

	for _, md := range mds {
		info, err := common.StringToTag(md)
		if err != nil {
			return nil, errors.Wrap(err, "common.StringToTag")
		}
		infos = append(infos, info)
		schema, err := common.NewSchemaElementFromTagMap(info)
		if err != nil {
			return nil, errors.Wrap(err, "common.NewSchemaElementFromTagMap")
		}
		//schema.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		schemaList = append(schemaList, schema)
	}
	res := NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
