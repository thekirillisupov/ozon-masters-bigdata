from sklearn.linear_model import LogisticRegression
import sys
from sklearn.extarnals import joblib



path_train = sys.argv[1]
path_model = sys.argv[2]

df= pd.read_json(path_train,lines=True)
df_train = pd.DataFrame(df.features.tolist(), index= df.index)
df_train['label'] = df['label']
model = LogisticRegression(max_iter=100)
model.fit(df_train.drop(['label'], axis=1), df_train['label'])

joblib.dump(model, path_model)
                                                     
