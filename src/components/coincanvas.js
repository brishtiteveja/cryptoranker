import React, { useEffect } from 'react'

import { Stage, Layer, Text, Circle, Group } from 'react-konva';
import Konva from 'konva';


let r = 8
class ColoredCircle extends React.Component {
    state = {
      key: '',
      symbol: '',
      i: 0,
      x: 200,
      y: 100,
      radius: 50,
      color: Konva.Util.getRandomColor(),
    };

    changeSize = () => {
        // to() is a method of `Konva.Node` instances
        this.circle.to({
          scaleX: Math.random() + 0.8,
          scaleY: Math.random() + 0.8,
          duration: 0.2,
        });
      };

    constructor(props) {
        super(props);
        // Don't call this.setState() here!
        this.state = { i: props.i, x: props.x, y:props.y, radius:props.radius, color:props.color};
        this.handleClick = this.handleClick.bind(this);
      }

    handleClick = () => {
      this.setState({
        color: Konva.Util.getRandomColor(),
      });
    };
    render() {
      return (
        <Group>
            <Circle
                key={this.state.key}
                ref={(node) => {
                    this.circle = node;
                }}
                x={this.state.x} y={this.state.y} radius={this.state.radius}
                fill={this.state.color}
                shadowBlur={5}
                onClick={this.handleClick}
                // draggable
                // onDragEnd={this.changeSize}
                // onDragStart={this.changeSize}
            />
        </Group>
      );
    }
  }

const CoinCanvas = ({ data }) => {

    function getRandomColor() {
        var r = Math.floor(Math.random(150,256) * 256);
        var g = Math.floor(Math.random(150,256) * 256);
        var b = Math.floor(Math.random(150,256) * 200);
        var a = 240 //Math.floor(Math.random() * 256);
        // var color = "#" + ((1 << 24) * Math.random() | 0).toString(16).padStart(6, "0")
        var color = "rgba(" + r + "," + g + "," + b + "," + a + ")";
        return color;
      }

    useEffect(() => {
        console.log(' data in canvas')
        // drawOnCanvas();
    })
    return(
      <div className="flex">

        {/* <div className="flex flex-col items-center justify-center">
            <canvas id="myCanvas" width="400" height="200">
            </canvas>
        </div> */}

        <div className="flex justify-center">
            <Stage width={window.innerWidth} height={800}>
                <Layer>
                    {
                        data.slice(0,50).map((coin, index) => (
                            <Group draggable>
                                <ColoredCircle 
                                    key={coin.id} 
                                    i={index} 
                                    symbol={coin.symbol} 
                                    x={200 + (index % r) * 150 + Math.floor(Math.random(0,20) * 20)} 
                                    y={100 + Math.floor(index / r) * 100 + Math.floor(Math.random(0,20) * 20)} 
                                    radius={50} 
                                    color={getRandomColor()}/>
                                <Text 
                                    key={"text_" + coin.id}
                                    text={coin.symbol} 
                                    x={185 + (index % r) * 150 } 
                                    y={ Math.floor(index / r) * 100 + 90} 
                                    fontStyle="bold" 
                                    fontSize="18" />
                            </Group>
                        )) 
                    }
                </Layer>
            </Stage>
        </div>
      </div>
    )
}

export default CoinCanvas;