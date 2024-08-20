import React, { useEffect, useState } from 'react';
import AvocatsEvolutionChart from './components/AvocatsEvolutionChart';

function App() {
  const [data, setData] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    console.log("Fetching data...");
    fetch("/avocats_analysis.json")
      .then(response => {
        console.log("Response received:", response);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
      })
      .then(data => {
        console.log("Data received:", data);
        setData(data);
      })
      .catch(e => {
        console.error("Error fetching data:", e);
        setError(e.message);
      });
  }, []);

  return (
    <div className="App">
      <h1>Évolution du nombre d'avocats en France</h1>
      {error && <p>Error: {error}</p>}
      {data.length === 0 ? (
        <p>Loading data...</p>
      ) : (
        <>
          <AvocatsEvolutionChart data={data} />
          <h2>Raw Data:</h2>
          <pre>{JSON.stringify(data, null, 2)}</pre>
        </>
      )}
    </div>
  );
}

export default App;