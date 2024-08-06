from yato import Transformation
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

class AvocatsAnalysis(Transformation):
    @staticmethod
    def source_sql():
        return "SELECT * FROM avocats.avocats_evolution"

    def run(self, context, *args, **kwargs):
        df = self.get_source(context)
        
        df_melted = pd.melt(df, id_vars=['name', 'address', 'cour_appel'], 
                            var_name='year', value_name='avocats_count')
        df_melted['year'] = df_melted['year'].str.extract('(\d+)').astype(int)
        
        df_total = df_melted.groupby('year')['avocats_count'].sum().reset_index()
        
        X = df_total['year'].values.reshape(-1, 1)
        y = df_total['avocats_count'].values
        
        model = LinearRegression()
        model.fit(X, y)
        
        future_years = np.array(range(df_total['year'].max() + 1, df_total['year'].max() + 6)).reshape(-1, 1)
        future_predictions = model.predict(future_years)
        
        future_df = pd.DataFrame({
            'year': future_years.flatten(),
            'avocats_count': future_predictions.astype(int)
        })
        
        result_df = pd.concat([df_total, future_df])
        result_df['is_prediction'] = result_df['year'] > df_total['year'].max()
        
        return result_df