# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Estimator, Model, Pipeline, PipelineModel
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F


class HasOutputColsPrefix(Params):

    output_prefix = Param(Params._dummy(), "output_prefix", 
                         "prefix for every output column name",
                         typeConverter=TypeConverters.toString)

    def __init__(self):
        super(HasOutputColsPrefix, self).__init__()
        self._setDefault(output_prefix='is')
    
    def setOutputColsPrefix(self, value):
        return self._set(output_prefix=value)

    def getOutputColsPrefix(self):
        return self.getOrDefault(self.output_prefix)


class HasFieldValues(Params):

    field_values = Param(Params._dummy(), "field_values", 
                         "all possible values for a field",
                         typeConverter=TypeConverters.toList)

    def __init__(self):
        super(HasFieldValues, self).__init__()

    def setFieldValues(self, value):
        return self._set(field_values=value)

    def getFieldValues(self):
        return self.getOrDefault(self.field_values)


class StringDisassembleModel(Model, HasInputCol, 
                             HasOutputColsPrefix, HasFieldValues, 
                             DefaultParamsReadable, DefaultParamsWritable):
    
    def get_new_cols(self):
        x = self.getInputCol()
        prefix = self.getOutputColsPrefix()
        field_values = self.getFieldValues()
        return ['{}_{}_{}'.format(prefix, x, v) for v in field_values]
    
    def disassemble_row(self, field_values, row):
        x = self.getInputCol()
        prefix = self.getOutputColsPrefix()
        new_data = {'{}_{}_{}'.format(prefix, x, v): float(row[x]==v)
                     for v in field_values}
        data = row.asDict()
        data.update(new_data)
        return Row(**data) 
    
    def _transform(self, dataset):
        cols = dataset.columns + self.get_new_cols()
        field_values = self.getFieldValues()
        disassemble_func = functools.partial(self.disassemble_row, field_values)
        return dataset.rdd \
            .map(disassemble_func) \
            .toDF() \
            .select(*cols)


class StringDisassembler(Estimator, HasInputCol, HasOutputColsPrefix):
    
    def _fit(self, dataset):
        x = self.getInputCol()
        field_values = dataset.rdd \
            .map(lambda r: r[x]) \
            .distinct() \
            .filter(lambda v: v is not None) \
            .collect()
        model = StringDisassembleModel() \
            .setInputCol(self.getInputCol()) \
            .setOutputColsPrefix(self.getOutputColsPrefix()) \
            .setFieldValues(field_values)

        return model
