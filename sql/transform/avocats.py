from yato import Transformation
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np

class Avocats(Transformation):
    @staticmethod
    def source_sql():
        return "SELECT * FROM avocats_data"

    def run(self, context, *args, **kwargs):
        df = self.get_source(context)
        
        # Melt the dataframe to have year as a column
        df_melted = pd.melt(df, id_vars=['name', 'address', 'cour_appel'], 
                            var_name='year', value_name='avocats_count')
        df_melted['year'] = df_melted['year'].str.extract('(\d+)').astype(int)
        
        # Group by year and sum the total number of avocats
        df_total = df_melted.groupby('year')['avocats_count'].sum().reset_index()
        
        # Predict for the next 5 years
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