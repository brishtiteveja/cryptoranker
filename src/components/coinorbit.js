import * as THREE from "three";
import { useEffect } from "react";
import SceneInit from "../lib/SceneInit";
import Planet from "../lib/Planet";
import Rotation from "../lib/Rotation";

const CoinOrbit = ({ data }) => {
  useEffect(() => {
    let gui;
    const initGui = async () => {
        const dat = await import("dat.gui");
        gui = new dat.GUI();
    };
    async function sceneMange() {
        let test = new SceneInit();
        test.initScene();
        test.animate();

        const solarSystem = new THREE.Group();
        await initGui();
        const solarSystemGui = gui.addFolder("solar system");
        for(let i = 0; i < 100; i++) {
            const planet = new Planet(1, i * 2.3, "bitcoin.jpg")
            const planetMesh = planet.getMesh();

            solarSystem.add(planetMesh);

            const planetRotation = new Rotation(planetMesh)
            const planetRotationMesh = planetRotation.getMesh();
            solarSystemGui.add(planetRotationMesh, "visible").name("mercury").listen();
        }
        test.scene.add(solarSystem);
        

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
