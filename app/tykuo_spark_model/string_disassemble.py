# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import functools

from pyspark.ml.pipeline import Estimator, Model
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F
from pyspark.sql import Row

from params import HasOutputColsPrefix, HasFieldValues, FillMode


class StringDisassembleModel(Model, HasInputCol, HasOutputCols, 
                             HasFieldValues, FillMode, 
                             DefaultParamsReadable, DefaultParamsWritable):
    
    def getMode(self):
        values = self.getFieldValues()
        return None if len(values) == 0 else values[0]
    
    @staticmethod
    def disassemble_row(params, row):
        category = row[params['input']]
        if category is None and params['fill_mode']:
            category = params['mode']
            
        new_data = {f: float(category==v)
                    for f, v in params['b_fv_list'].value}
        
        data = row.asDict()
        data.update(new_data)
        return Row(**data) 
    
    def _transform(self, dataset):
        fields = self.getOutputCols()
        values = self.getFieldValues()
        sc = dataset.rdd.context
        b_fv_list = sc.broadcast(zip(fields, values))
       
        dismb_params = {
            'input': self.getInputCol(),
            'b_fv_list': sc.broadcast(zip(fields, values)),
            'fill_mode': self.getFillMode(),
            'mode': self.getMode()
        }
        
        disassemble_func = functools \
            .partial(self.disassemble_row, dismb_params)
        
        cols = dataset.columns + fields
        return dataset.rdd \
            .map(disassemble_func) \
            .toDF() \
            .select(*cols)


class StringDisassembler(Estimator, HasInputCol, HasOutputColsPrefix, FillMode):
    
    def get_values(self, dataset):
        x = self.getInputCol()
        # values ordered by count (mode)
        values = dataset \
            .where('{} is not null'.format(x)) \
            .groupBy(x) \
            .agg(F.count('*').alias('count')) \
            .orderBy(F.desc('count')) \
            .rdd.map(lambda r: r[x]) \
            .collect()
            
        return values
    
    def get_fields(self, values):
        x = self.getInputCol()
        prefix = self.getOutputColsPrefix()
        return ['{}_{}_{}'.format(prefix, x, v) for v in values]
    
    def _fit(self, dataset):
        x = self.getInputCol()
        values = self.get_values(dataset)
        fields = self.get_fields(values)
        model = StringDisassembleModel() \
            .setInputCol(x) \
            .setOutputCols(fields) \
            .setFieldValues(values) \
            .setFillMode(self.getFillMode())

        return model