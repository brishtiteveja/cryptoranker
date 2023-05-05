import * as THREE from "three";
import { useEffect } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";
import Rotation from "../lib/Rotation";

const CoinOrbit = ({ data }) => {
  useEffect(() => {
    async function sceneMange() {
        let sceneManager = new SceneInit();
        sceneManager.initScene();
        sceneManager.animate();

        const solarSystem = new THREE.Group();
        for(let i = 0; i < 10; i++) {
            const planet = new Planet(1, i * 2.3, "bitcoin.jpg")
            const planetMesh = planet.getMesh();

            solarSystem.add(planetMesh);

            const planetRotation = new Rotation(planetMesh)
            const planetRotationMesh = planetRotation.getMesh();
        }
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
