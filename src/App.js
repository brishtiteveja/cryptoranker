import Header from './components/header'
import CoinTable from './components/cointable'
import CoinCanvas from './components/coincanvas'

// var cors = require('cors')

function App() {
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

              <p className="text-center text-3lg font-bold">
                Rank cryptocoins
              </p>
              <CoinCanvas />
              <CoinTable />
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
