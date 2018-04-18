# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from pyspark.ml.param.shared import *


class HasModeDict(Params):
    
    mode_dict = Param(Params._dummy(),
            "mode_dict", "mode for every column")

    def __init__(self):
        super(HasModeDict, self).__init__()

    def setModeDict(self, value):
        return self._set(mode_dict=value)

    def getModeDict(self):
        return self.getOrDefault(self.mode_dict)


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