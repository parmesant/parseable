/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use once_cell::sync::Lazy;
use chrono::{DateTime, Utc};
use chrono::{NaiveDateTime, TimeZone};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList, PyModule};


pub static PANORAMA_STATIC: Lazy<Panorama> = Lazy::new(|| Panorama{});

// A query request by client
#[derive(Debug)]
pub struct Panorama {

}


impl Panorama {
    pub fn detect_anomaly(
        &self,
        py: Python,
        stream: String,
        start: DateTime<Utc>,
        end: DateTime<Utc>
    ) -> PyResult<()> {
        let file_path = "/home/anant/projects/anomaly-test/assets/frontend-logs.csv";

        let df = self.load_and_preprocess_data(py, file_path, 404, "2s")?;

        let model = self.fit_prophet_model(py, df.clone_ref(py))?;

        let forecast = self.forecast_prophet_model(py, model, 30)?;

        let df_with_residuals = self.calculated_residuals(py, df, forecast)?;

        let residuals = df_with_residuals.call_method1(py, "get", ("residual",))?;
        let ds_col = df_with_residuals.call_method1(py, "get", ("ds",))?;

        println!("residuals and ds_col made");

        let residuals_list: Vec<f64> = residuals.call_method0(py, "tolist")?.extract(py)?;
        let residuals_py = PyList::new_bound(py, &residuals_list).into();

        let ds_list: Vec<String> = ds_col.call_method0(py, "tolist")?.extract(py)?;

        let z_scores = self.calculate_z_scores(py, residuals_py)?;

        let anomalies = self.detect_anomalies(py, z_scores, 3.0)?;

        let anomalies_list: Vec<usize> = anomalies.extract(py)?;
        let anomaly_dates: Vec<String> = anomalies_list
            .iter()
            .filter_map(|&index| ds_list.get(index).cloned())
            .collect();

        println!("Anomalies detected at: {:?}", anomaly_dates);

        Ok(())
    }

    /// The data needs to be fed in a different manner
    /// Prophet requires date and a numerical column
    /// our numerical column is going to be an aggregation of
    /// total calls made in a given time-period
    /// `status_code` is not numerical, it is discrete
    fn load_and_preprocess_data(&self, py: Python, file_path: &str, status_code: i32, freq: &str) -> PyResult<Py<PyAny>> {

        let code = r#"
import pandas as pd

def load_and_preprocess_data(filepath, status_code=404, freq='2s'):
    df = pd.read_csv(filepath, parse_dates=['datetime'])

    df_filtered = df[df['status'] == status_code]
    df_filtered['datetime'] = df_filtered['datetime'].dt.tz_localize(None)
    df_filtered = df_filtered.set_index('datetime').resample(freq).size().reset_index(name='y')
    df_filtered.rename(columns={'datetime': 'ds'}, inplace=True)
    print(df_filtered)
    return df_filtered
"#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
    
        let res_function = res_module.getattr("load_and_preprocess_data")?;
    
        let result= res_function.call1((file_path, status_code, freq))?;
    
        let df: Py<PyAny> = result.extract()?;
    
        Ok(df)
    }


    fn fit_prophet_model(&self, py: Python, df: Py<PyAny>) -> PyResult<Py<PyAny>>{
        let code = r#"
from prophet import Prophet

def fit_prophet_model(df):
    model = Prophet()
    model.fit(df)
    return model
    "#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
    
        let res_function = res_module.getattr("fit_prophet_model")?;
    
        let result = res_function.call1((df,))?;
    
        let model: Py<PyAny> = result.extract()?;
    
        Ok(model)
    }
    
    fn forecast_prophet_model(&self, py: Python, df: Py<PyAny>, periods: i32) -> PyResult<Py<PyAny>>{
        let code = r#"
from prophet import Prophet

def forecast_prophet_model(model, periods):
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)
    return forecast
    "#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
    
        let res_function = res_module.getattr("forecast_prophet_model")?;
    
        let result = res_function.call1((df, periods))?;
    
        let forecast: Py<PyAny> = result.extract()?;
    
        Ok(forecast)
    }


    fn calculated_residuals(&self, py: Python, df: Py<PyAny>, forecast: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let code = r#"
import numpy as np
import pandas as pd

def calculated_residuals(df, forecast):
    df_merged = df.merge(forecast[['ds', 'yhat']],on='ds')
    df_merged['residual'] = df_merged['y'] - df_merged['yhat']
    df_merged['ds'] = df_merged.ds.dt.strftime('%Y-%m-%d %H:%M:%S')
    return df_merged
    "#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
    
        let res_function = res_module.getattr("calculated_residuals")?;
    
        let result = res_function.call1((df, forecast))?;
    
        let residuals: Py<PyAny> = result.extract()?;
    
        Ok(residuals)
    }
    
    fn calculate_z_scores(&self, py: Python, residuals: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let code = r#"
import numpy as np

def calculate_z_scores(residuals):
    mean_residual = np.mean(residuals)
    std_residual = np.std(residuals)
    z_scores = (residuals - mean_residual) / std_residual
    return z_scores
    "#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
        let res_function = res_module.getattr("calculate_z_scores")?;
        let result = res_function.call1((residuals,))?;
        
        let z_scores: Py<PyAny> = result.extract()?;
        Ok(z_scores)
    }
    
    fn detect_anomalies(&self, py: Python, z_scores: Py<PyAny>, threshold: f64) -> PyResult<Py<PyAny>> {
        let code = r#"
import numpy as np

def detect_anomalies(z_scores, threshold=3):
    return np.where(np.abs(z_scores) > threshold)[0]
    "#;
    
        let res_module = PyModule::from_code_bound(py, code, "", "")?;
        let res_function = res_module.getattr("detect_anomalies")?;
        let result = res_function.call1((z_scores, threshold))?;
        
        let anomalies: Py<PyAny> = result.extract()?;
        Ok(anomalies)
    }
}