import Header from './components/header'
import CoinTable from './components/cointable'
// import CoinCanvas from './components/coincanvas'
import CoinOrbit from './components/coinorbit'

import { useState } from 'react'

// var cors = require('cors')

function App() {
  const [data, setData] = useState([])

  return (
    <div className="flex-col justify-between m-2">
      {/* header */}
      <Header />
      
      {/* main content */}
      <div className="flex">
        {/* left side */}
        <div className="flex">

        </div>

        {/* main content */}
        <main className="flex">
          <div className="flex-col mt-10">
              <CoinOrbit data={data} />
              <p className="text-center text-3lg font-bold">
                Rank cryptocoins
              </p>
              {/* <CoinCanvas data={data} /> */}
              {/* <CoinTable data={data} setData={setData} /> */}
          </div>
        </main>

        {/* right side */}
        <div className="flex">

        </div>

      </div>

      {/* footer */}
      <div className="flex">

      </div>

    </div>
  );
}

export default App;
