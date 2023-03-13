/* eslint-disable react/jsx-key */
import React from "react";
import { useTable } from "react-table";

// Create a default prop getter
const defaultPropGetter = () => ({});

function Table({ 
                columns, 
                data,
                getHeaderProps = defaultPropGetter,
                getColumnProps = defaultPropGetter,
                getRowProps = defaultPropGetter,
                getCellProps = defaultPropGetter
               }) {
  // Use the state and functions returned from useTable to build your UI
  const { 
    getTableProps, 
    getTableBodyProps, 
    headerGroups, 
    rows, 
    prepareRow 
  } = useTable({
      columns,
      data,
    });

  // Render the UI for your table
  return (
    <table {...getTableProps()} border="1">
      <thead>
        {headerGroups.map((headerGroup) => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {
              headerGroup.headers.map((column) => (
                <th {...column.getHeaderProps()}>
                  {column.render("Header")}
                </th>
              ))
            }
          </tr>
        ))}
      </thead>
      <tbody {...getTableBodyProps()}>
        {rows.map((row, i) => {
          prepareRow(row);
          return (
            <tr {...row.getRowProps()}>
              {
                row.cells.map((cell) => {
                  return (
                    <td {...cell.getCellProps(
                      [
                        {
                          className: cell.column.className,
                          style: cell.column.style
                        },
                        getColumnProps(cell.column),
                        getCellProps(cell)
                      ]
                    )}>
                      {cell.render("Cell")}
                    </td>
                  );
                })
              }
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export default Table;