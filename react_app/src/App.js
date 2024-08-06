import React, { useEffect, useState } from 'react';
import AvocatsEvolutionChart from './components/AvocatsEvolutionChart';

function App() {
  const [data, setData] = useState([]);

  useEffect(() => {
    fetch("/avocats_analysis.json")
      .then(response => response.json())
      .then(setData);
  }, []);

  return (
    <div className="App">
      <h1>Évolution du nombre d'avocats en France</h1>
      <AvocatsEvolutionChart data={data} />
    </div>
  );
}

export default App;