# custom-spark-pipeline

# app/tykuo_spark_model
## ModeImputer
* Impute categorical features with mode

## StringDisassembler (OneHot)
* Disassemble categorical feature into multiple binary columns 

## VectorDisassembler
* Disassemble vector feature into multiple numeric columns 

## ConstantImputer
* Impute NA with constant (string, number or dict)

# Examples
## mode-imputer
* Impute categorical features with mode
* Combine with spark 2.3 imputer into savable pipeline

## custom-onehot
* StringDisassembler vs OneHotEncoderEstimator

## vec-disassembler
* Try VectorDisassembler

## constant-imputer
* Try ConstantImputer

## full-process-pipeline
* Put all custom feature estimators together

