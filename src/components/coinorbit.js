import * as THREE from "three";
import { useEffect, useRef } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";

const CoinOrbit = ({ marketData }) => {
  const sceneRef = useRef(null);

  useEffect(() => {
    async function sceneManage(marketData) {
      let sceneManager = new SceneInit();
      sceneManager.initScene();
      sceneManager.animate();

      const randomColor = () => {
        const colors = [
          'red', 'blue', 'green', 'yellow', 'purple', 'orange', 'cyan', 'magenta', 'lime', 'pink'
        ];
        return colors[Math.floor(Math.random() * colors.length)];
      };

      let maxMarketCap = Math.max(...marketData.map(item => item.market_cap));

      // Find minimum market_cap
      let minMarketCap = Math.min(...marketData.map(item => item.market_cap));

      // Find maximum total_volume
      let maxTotalVolume = Math.max(...marketData.map(item => item.total_volume));

      // Find minimum total_volume
      let minTotalVolume = Math.min(...marketData.map(item => item.total_volume));

      const generatePlanetData = (count) => {
        const planetData = [];
        for (let i = 0; i < count; i++) {
          
          const orbitRadius = 10 + i * 5;
          const orbitSpeed = Math.random() * 0.002;
          const curMarketCap = marketData[i].market_cap
          let planetSize = 10;
          if (maxMarketCap !== minMarketCap)
            planetSize = Math.abs((curMarketCap - minMarketCap) / (maxMarketCap - minMarketCap)) * 10 + 1;

          const radius = planetSize;
          const color = randomColor();

          let textureFile = "cardano.jpg";

          let planetInfo = {}

          if (marketData[i] !== undefined && marketData[i].hasOwnProperty('id')) {
            let crypto_symbol = marketData[i].symbol;
            textureFile = 'crypto_icons/' + crypto_symbol.toUpperCase() + '.png';
            console.log(textureFile)

            // do something with data[i]
            planetInfo = {
              name: crypto_symbol,
              mass: `${Math.random() * 1000} kg`,
              distance: `${orbitRadius} AU`,
              orbitalPeriod: `${Math.random() * 100} years`
            };
          } else {
            planetInfo = {
              name: `Planet ${i + 1}`,
              mass: `${Math.random() * 1000} kg`,
              distance: `${orbitRadius} AU`,
              orbitalPeriod: `${Math.random() * 100} years`
            };

          }

          planetData.push([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo]);
        }

        return planetData;
      };

      const planetData = generatePlanetData(100);


      const planets = planetData.map(([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo], index) => {
        const planet = new Planet(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo);
        const planetMesh = planet.getMesh(sceneManager.scene);

        planetMesh.name = `planet-${index}`;

        sceneManager.scene.add(planetMesh);

        planet.drawOrbit(sceneManager.scene, color, index + 1);

        return planet;
      });

      sceneManager.planets = planets;

      const handlePlanetClick = (event) => {
        const canvasBounds = sceneRef.current.getBoundingClientRect();
        const x = event.clientX - canvasBounds.left;
        const y = event.clientY - canvasBounds.top;
        const mouse = new THREE.Vector2((x / canvasBounds.width) * 2 - 1, -(y / canvasBounds.height) * 2 + 1);

        const raycaster = new THREE.Raycaster();
        raycaster.setFromCamera(mouse, sceneManager.camera);

        const intersects = raycaster.intersectObjects(sceneManager.scene.children, true);

        for (const intersect of intersects) {
          if (intersect.object.name.startsWith("planet-")) {
            const planetIndex = parseInt(intersect.object.name.split("-")[1]);
            const planet = planets[planetIndex];
            planet.displayPlanetInfo();
          }
        }
      };

      sceneManager.renderer.domElement.addEventListener("click", handlePlanetClick);
    }

    sceneManage(marketData);
  }, [marketData]);

  return (
    <div id="canvasParent" className="flex flex-col items-center justify-center">
      <canvas id="myThreeJsCanvas" className="ml-20" ref={sceneRef} />
    </div>
  );
};

export default CoinOrbit;
