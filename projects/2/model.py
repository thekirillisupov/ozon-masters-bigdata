from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder, Normalizer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV

#
# Dataset fields
#
numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]
fields = ["id", "label"] + numeric_features + categorical_features
fieldswithoutlabel = ["id"] + numeric_features + categorical_features
#
# Model pipeline
#
#categorical_features = ["cf6","cf9","cf14","cf17","cf20","cf22","cf23"]


def select(X):
  ans=[]
  s={"cf"+str(i) for i in range(1,27)}
  s.add("day_number")
  for x in X:
    if x in s:
      if len(X[x].unique()) < 20:
        ans.append(x)
  return ans

# We create the preprocessing pipelines for both numeric and categorical data.
numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', Normalizer())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, select)
    ],
)

# Now we have a full prediction pipeline.
model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('linearregression', LogisticRegression(max_iter=100, solver='sag',penalty='l2',C=0.9))
])


