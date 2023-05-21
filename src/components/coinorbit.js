import * as THREE from "three";
import { useEffect, useRef } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";

const CoinOrbit = ({ data }) => {
  const sceneRef = useRef(null);

  useEffect(() => {
    async function sceneManage() {
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
          
          const orbitRadius = 10 + i * 5;
          const orbitSpeed = Math.random() * 0.002;
          const planetSize = 0.5 + Math.random() * 5;
          const radius = planetSize;
          const color = randomColor();
          const textureFile = "bitcoin.jpg";
          const planetInfo = {
            name: `Planet ${i + 1}`,
            mass: `${Math.random() * 1000} kg`,
            distance: `${orbitRadius} AU`,
            orbitalPeriod: `${Math.random() * 100} years`
          };

          data.push([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo]);
        }

        return data;
      };

      const planetData = generatePlanetData(100);


      const planets = planetData.map(([radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo], index) => {
        const planet = new Planet(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo);
        const planetMesh = planet.getMesh();

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

    sceneManage();
  }, []);

  return (
    <div id="canvasParent" className="flex flex-col items-center justify-center">
      <canvas id="myThreeJsCanvas" className="ml-20" ref={sceneRef} />
    </div>
  );
};

export default CoinOrbit;
