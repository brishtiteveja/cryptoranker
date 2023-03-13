/* eslint-disable react/jsx-key */
'use client'
import React, { useState, useEffect } from 'react' 
import axios from 'axios'
import Table from './table'

const CoinTable = () => {
    const [coinList, setCoinList] = useState([])
    const [data, setData] = useState([])

    const BASE_API_URL = "https://api.coingecko.com/api/v3/"
    const COIN_LIST_API_URL = BASE_API_URL + "coins/list"
    const MARKET_DATA_API_URL = BASE_API_URL + "coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250"

    const toCurrency = (numberString) => {
      let number = parseFloat(numberString);
      return number.toLocaleString('USD');
    }

    const columns = React.useMemo(
        () => [
          {
            Header: "#",
            accessor: "rank",
          },
          {
            Header: "Name",
            accessor: "name",
          },
          {
            Header: "Price",
            accessor: "current_price",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)  
          },
          {
            Header: "Market Cap (USD)",
            accessor: "market_cap",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
        ],
        []
      );

    // fetch data
    useEffect( () => {
      axios.get(MARKET_DATA_API_URL).then(function (response) {
        let res = response.data
        res = res.map((item, index) => {
          item["rank"] = index + 1
          return item
        })
        setData(res)
        // console.log(res)
      });
    }, [MARKET_DATA_API_URL])
    
    // refresh by fetching data every 120 seconds
    setTimeout(() => {
        // get the coinlist
        if (coinList.length === 0) {
            axios.get(COIN_LIST_API_URL).then(function (response) {
                let res = response.data
                setCoinList(res)
            });
        }

        // get market data
        axios.get(MARKET_DATA_API_URL).then(function (response) {
            let res = response.data
            res = res.map((item, index) => {
              item["rank"] = index + 1
              return item
            })
            setData(res)
            //console.log(res)
        });
    }, 60000);

    // Render the UI for your table
  return (
    <div className="flex justify-center">
      {
        data && data.length > 0 ? (
            <Table 
              columns={columns} 
              data={data} 
              // getCellProps={(cellInfo) => ({
              //   style: {
              //     backgroundColor: cellInfo.value > 10000000000 ? "red" : null,
              //     opacity: cellInfo.value > 10000000000 ? 0.3 : 1
              //   }
              // })}  
            />
        ): <h2> Fetching data ...</h2>
      }
    </div>
  );
}

export default CoinTable;