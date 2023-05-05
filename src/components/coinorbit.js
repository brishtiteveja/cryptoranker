import * as THREE from "three";
import { useEffect } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";

const CoinOrbit = ({ data }) => {
  useEffect(() => {
    async function sceneMange() {
        let sceneManager = new SceneInit();
        sceneManager.initScene();
        sceneManager.animate();

        const randomColor = () => {
          const colors = [
            'red', 'blue', 'green', 'yellow', 'purple', 'orange', 'cyan', 'magenta', 'lime', 'pink'
          ];
          return colors[Math.floor(Math.random() * colors.length)];
        };

        const generatePlanetData = (count) => {
          const data = [];
          for (let i = 0; i < count; i++) {
            const radius = 1;
            const orbitRadius = 10 + i * 5;
            const orbitSpeed = Math.random() * 0.005;
            const planetSize = 0.2 + Math.random() * 0.8;
            const color = randomColor();
            const textureFile = "bitcoin.jpg"

            data.push([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile]);
          }

          return data;
        };

        const planetData = generatePlanetData(10);

        const solarSystem = new THREE.Group()
        const planets = planetData.map(([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile]) => {
          const planet = new Planet(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile)
          const planetMesh = planet.getMesh();

          solarSystem.add(planetMesh);

          return { planetMesh }
        })

        sceneManager.scene.planets = planets
        
        sceneManager.scene.add(solarSystem);
    }
    
    sceneMange();
  }, []);

  return (
    <div className="flex flex-col items-center justify-center">
      <canvas id="myThreeJsCanvas" className="ml-20" />
    </div>
  );
}

export default CoinOrbit;
