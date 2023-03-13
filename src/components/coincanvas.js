import React, { useRef, useState, useEffect } from 'react'
import { Canvas, useFrame } from '@react-three/fiber'

import { Stage, Layer, Rect, Text, Circle, Group } from 'react-konva';
import Konva from 'konva';

class ColoredCircle extends React.Component {
    state = {
      color: Konva.Util.getRandomColor(),
      x: 200,
      y: 100,
      radius: 50,
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
        this.state = { x: props.x, y:props.y, radius:props.radius, color:props.color};
        this.handleClick = this.handleClick.bind(this);
      }

    handleClick = () => {
      this.setState({
        color: Konva.Util.getRandomColor(),
      });
    };
    render() {
      return (
        <Circle
            ref={(node) => {
                this.circle = node;
            }}
            x={this.state.x} y={this.state.y} radius={this.state.radius}
          fill={this.state.color}
          shadowBlur={5}
          onClick={this.handleClick}
          draggable
        onDragEnd={this.changeSize}
        onDragStart={this.changeSize}
        />
      );
    }
  }


class ColoredRect extends React.Component {
    state = {
      color: 'green',
    };
    handleClick = () => {
      this.setState({
        color: Konva.Util.getRandomColor(),
      });
    };
    render() {
      return (
        <Rect
          x={20}
          y={20}
          width={50}
          height={50}
          fill={this.state.color}
          shadowBlur={5}
          onClick={this.handleClick}
        />
      );
    }
  }

function Box(props) {
  // This reference gives us direct access to the THREE.Mesh object
  const ref = useRef()
  // Hold state for hovered and clicked events
  const [hovered, hover] = useState(false)
  const [clicked, click] = useState(false)
  // Subscribe this component to the render-loop, rotate the mesh every frame
  useFrame((state, delta) => (ref.current.rotation.x += delta))
  // Return the view, these are regular Threejs elements expressed in JSX
  return (
    <mesh
      {...props}
      ref={ref}
      scale={clicked ? 1.5 : 1}
      onClick={(event) => click(!clicked)}
      onPointerOver={(event) => hover(true)}
      onPointerOut={(event) => hover(false)}>
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial color={hovered ? 'hotpink' : 'orange'} />
    </mesh>
  )
}

function drawOnCanvas() {
    var canvas = document.getElementById('#canvas')[0];
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    
    if(canvas.getContext) {
      var ctx = canvas.getContext('2d');
      var w = canvas.width;
      var h = canvas.height;
      ctx.strokeStyle = 'rgba(174,194,224,0.5)';
      ctx.lineWidth = 10;
      ctx.lineCap = 'round';
      
      
      var init = [];
      var maxParts = 10;
      for(var a = 0; a < maxParts; a++) {
        init.push({
          x: Math.random() * w,
          y: Math.random() * h,
          l: Math.random() * 1,
          xs: -4 + Math.random() * 4 + 2,
          ys: Math.random() * 10 + 10
        })
      }
      
      var particles = [];
      for(var b = 0; b < maxParts; b++) {
        particles[b] = init[b];
      }
      
      function draw() {
        ctx.clearRect(0, 0, w, h);
        for(var c = 0; c < particles.length; c++) {
          var p = particles[c];
          ctx.beginPath();
          ctx.moveTo(p.x, p.y);
          ctx.lineTo(p.x + p.l * p.xs, p.y + p.l * p.ys);
          ctx.stroke();
        }
        move();
      }
      
      function move() {
        for(var b = 0; b < particles.length; b++) {
          var p = particles[b];
          p.x += p.xs;
          p.y += p.ys;
          if(p.x > w || p.y > h) {
            p.x = Math.random() * w;
            p.y = -20;
          }
        }
      }
      
      setInterval(draw, 30);
      
    }
  }

const CoinCanvas = () => {

    // useEffect(() => {
    //     drawOnCanvas();
    // })
    return(
      <div>

        {/* <div>
            <canvas id="canvas" width="400" height="200">
            </canvas>
        </div> */}

        {/* <div className="flex justify-center">
            <Stage width={window.innerWidth} height={400}>
                <Layer>
                    {/* <Text text="Try click on rect" />
                    <ColoredCircle x={200} y={100} radius={50} color="green" />
                    <ColoredCircle x={400} y={100} radius={50}  color="red"/>
                    <ColoredCircle x={600} y={100} radius={50} color="blue" /> */}

                    {/* <Group draggable>
                        <Circle x={200} y={100} radius={50} fill="green" />
                        <Text text="Bitcoin" x={185} y={90} fontStyle="bold" />
                    </Group>

                    <Group draggable>
                        <Circle x={400} y={100} radius={50} fill="red" />
                        <Text text="Ethereum" x={385} y={90} fontStyle="bold" />
                    </Group>

                    <Group draggable>
                        <Circle x={600} y={100} radius={50} fill="red" />
                        <Text text="BNB" x={585} y={90} fontStyle="bold" />
                    </Group>

                </Layer> */}
            {/* </Stage>
        </div> */}
      </div>
    )
}

export default CoinCanvas;