# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.pipeline import Estimator, Model, Pipeline, PipelineModel
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.param.shared import *
import pyspark.sql.functions as F

from params import HasModeDict


class ModeImputerModel(Model, 
    HasInputCols, HasOutputCols, HasModeDict, 
    DefaultParamsReadable, DefaultParamsWritable):
    
    def _transform(self, dataset):
        xs = self.getInputCols()
        ys = self.getOutputCols()
        mode_dict = self.getModeDict()
        imputed_df = dataset
        for x, y in zip(xs, ys):
            imputed_df = imputed_df \
                .withColumn(y, F.when(F.col(x).isNull(), mode_dict[x]).otherwise(F.col(x)))
        return imputed_df


class ModeImputer(Estimator, HasInputCols, HasOutputCols):
    
    def prepare_io_params(self):
        xs = self.getInputCols()
        ys = []
        try:
            ys = self.getOutputCols()
        except:
            ys = []
        n = len(xs) - len(ys)
        if n > 0:
            ys = ys[:] + xs[-n:]
        elif n < 0:
            ys = ys[:n]
        return xs, ys
    
    def _fit(self, dataset):
        xs, ys = self.prepare_io_params()
        mode_dict = {}
        for c in xs:
            rows = dataset.where('{} is not null'.format(c)) \
                .groupBy(c) \
                .agg(F.count('*').alias('count')) \
                .orderBy(F.desc('count')) \
                .take(1) 
            if len(rows) > 0:
                mode_dict[c] = rows[0][c]
        impute_model = ModeImputerModel() \
            .setInputCols(xs) \
            .setOutputCols(ys) \
            .setModeDict(mode_dict)
            
        return impute_model