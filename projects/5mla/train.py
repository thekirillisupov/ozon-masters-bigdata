#!/opt/conda/envs/dsenv/bin/python
import mlflow
import os, sys
import pandas as pd
from sklearn.model_selection import train_test_split

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder, Normalizer
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV



numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]
fields = ["id", "label"] + numeric_features + categorical_features
fieldswithoutlabel = ["id"] + numeric_features + categorical_features

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


if __name__ == "__main__":
    train_path = sys.argv[1]
    paramC = sys.argv[2]

    read_table_opts = dict(sep="\t", names=fields, index_col=False)
    df = pd.read_table(train_path, **read_table_opts)
    arr_num = [i for i in range(15)]
    arr_cat = [20,23,28,31,34,36,37]
    arr_fin=arr_cat+arr_num
    X_train, X_test, y_train, y_test = train_test_split(
        df.iloc[:,2:], df.iloc[:,1], test_size=0.33, random_state=41
    )




    with mlflow.start_run():
        model.fit(X_train, y_train)
        model_score = model.score(X_test, y_test)
	mlflow.log_metric("model_param1", paramC)
        mlflow.log_metric("log_loss", model_score)
        mlflow.sklearn.log_model(model, "model")



