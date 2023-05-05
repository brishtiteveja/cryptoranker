import * as THREE from "three";
import { useEffect } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";
// import Rotation from "../lib/Rotation";

const CoinOrbit = ({ data }) => {
  useEffect(() => {
    async function sceneMange() {
        let sceneManager = new SceneInit();
        sceneManager.initScene();
        sceneManager.animate();

        const solarSystem = new THREE.Group();

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

        const planets = planetData.map(([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile]) => {
          const planet = new Planet(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile)
          const planetMesh = planet.getMesh();

          // const planetRotation = new Rotation(planetMesh)
          // const planetRotationMesh = planetRotation.getMesh();
          solarSystem.add(planetMesh);

          return { planetMesh }
        })
        
        sceneManager.scene.add(solarSystem);
        

        // NOTE: Animate solar system at 60fps.
        // const EARTH_YEAR = 2 * Math.PI * (1 / 60) * (1 / 60);
        // const animate = () => {
        //     sunMesh.rotation.y += 0.001;
        //     mercurySystem.rotation.y += EARTH_YEAR * 1.2;
        //     venusSystem.rotation.y += EARTH_YEAR * 1.5;
        //     earthSystem.rotation.y += EARTH_YEAR;
        //     marsSystem.rotation.y += EARTH_YEAR * 0.5;
        //     jupiterSystem.rotation.y += EARTH_YEAR * 0.25;
        //     requestAnimationFrame(animate);
        // };
        // animate();
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
