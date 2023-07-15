/* eslint-disable react/jsx-key */
'use client'
import React, { useState, useEffect } from 'react' 
import axios from 'axios'
import Table from './table'

const CoinTable = ({ data, setData }) => {
    const [coinList, setCoinList] = useState([])

    const [searchTerm, setSearchTerm] = useState('');
    const [filteredData, setFilteredData] = useState([]);

    const BASE_API_URL = "https://api.coingecko.com/api/v3/"
    const COIN_LIST_API_URL = BASE_API_URL + "coins/list"
    // const MARKET_DATA_API_URL = BASE_API_URL + "coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250"

    const vs_currency = "usd"
    const order = "market_cap_desc"
    const per_page = 100
    const page_id = 1
    const sparkline = false
    const price_change_percentage = '1h,24h,7d,30d,1y'
    const locale = 'en'
    const MARKET_PRICE_DATA_API_URL = BASE_API_URL
            + '/coins/markets' + '?'
            + 'vs_currency=' + vs_currency + '&'
            + 'order=' + order + '&'
            + 'per_page=' + per_page + '&'
            + 'page=' + page_id + '&'
            + 'sparkline=' + sparkline + '&'
            + 'price_change_percentage=' + price_change_percentage + '&'
            + 'locale=' + locale

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
          {
            Header: "24h volume",
            accessor: "total_volume",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
          {
            Header: "1h",
            accessor: "price_change_percentage_1h_in_currency",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
          {
            Header: "24h",
            accessor: "price_change_percentage_24h_in_currency",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
          {
            Header: "1W",
            accessor: "price_change_percentage_7d_in_currency",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
          {
            Header: "1M",
            accessor: "price_change_percentage_30d_in_currency",
            style: {
              textAlign: 'center'
            },
            Cell: props => toCurrency(props.value)
          },
          {
            Header: "1Y",
            accessor: "price_change_percentage_1y_in_currency",
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
      axios.get(MARKET_PRICE_DATA_API_URL).then(function (response) {
        let res = response.data
        res = res.map((item, index) => {
          item["rank"] = index + 1
          return item
        })
        setData(res)
        setFilteredData(res);
      });
    }, [MARKET_PRICE_DATA_API_URL, setData, setFilteredData])

    useEffect(() => {
      const filterData = () => {
        const filtered = data.filter((item) =>
          item.name.toLowerCase().includes(searchTerm.toLowerCase())
        );
        setFilteredData(filtered);
      };

      filterData();
    }, [data, searchTerm]);
    
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
        axios.get(MARKET_PRICE_DATA_API_URL).then(function (response) {
            let res = response.data
            res = res.map((item, index) => {
              item["rank"] = index + 1
              return item
            })
            setData(res)
        });
    }, 90000);

    // Render the UI for your table
  return (
    <div className="flex flex-col p-2 m-2">
      <div className="flex ml-30 p-2">
        <input
          className="p-2 justify-center w-1/2 float-right"
          type="text"
          placeholder="Search Cryptocurrency"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
      </div>
      {
        data && data.length > 0 ? (
          <div className="flex">
            <Table 
              columns={columns} 
              data={filteredData} 
              // getCellProps={(cellInfo) => ({
              //   style: {
              //     backgroundColor: cellInfo.value > 10000000000 ? "red" : null,
              //     opacity: cellInfo.value > 10000000000 ? 0.3 : 1
              //   }
              // })}  
            />
          </div>
        ): <h2> Fetching data ...</h2>
      }
    </div>
  );
}

export default CoinTable;