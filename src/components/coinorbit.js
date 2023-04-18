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

        const sunGeometry = new THREE.SphereGeometry(8);
        const sunTexture = new THREE.TextureLoader().load("bitcoin.jpg")// "bitcoin.jpg");
        const sunMaterial = new THREE.MeshBasicMaterial({ map: sunTexture });
        const sunMesh = new THREE.Mesh(sunGeometry, sunMaterial);
        const solarSystem = new THREE.Group();
        solarSystem.add(sunMesh);
        test.scene.add(solarSystem);

        const mercuryGeometry = new THREE.SphereGeometry(8);
        const mercuryTexture = new THREE.TextureLoader().load("xrp.jpg")
        const mercuryMaterial = new THREE.MeshBasicMaterial({ map: mercuryTexture });
        const mercuryMesh = new THREE.Mesh(mercuryGeometry, mercuryMaterial);
        mercuryMesh.position.x += 15
        const mercurySystem = new THREE.Group();
        mercurySystem.add(mercuryMesh);
        test.scene.add(solarSystem);

        // const mercury = new Planet(2, 16, "mercury.png");
        // const mercuryMesh = mercury.getMesh();
        // let mercurySystem = new THREE.Group();
        // mercurySystem.add(mercuryMesh);

        const venus = new Planet(3, 32, "venus.jpg");
        const venusMesh = venus.getMesh();
        let venusSystem = new THREE.Group();
        venusSystem.add(venusMesh);

        const earth = new Planet(4, 48, "earth.jpg");
        const earthMesh = earth.getMesh();
        let earthSystem = new THREE.Group();
        earthSystem.add(earthMesh);

        const mars = new Planet(3, 64, "mars.jpg");
        const marsMesh = mars.getMesh();
        let marsSystem = new THREE.Group();
        marsSystem.add(marsMesh);

        const jupiter = new Planet(3.5, 75, "jupiter.jpg");
        const jupiterMesh = mars.getMesh();
        let jupiterSystem = new THREE.Group();
        jupiterSystem.add(marsMesh);

        solarSystem.add(mercurySystem, venusSystem, earthSystem, marsSystem, jupiterSystem);

        const mercuryRotation = new Rotation(mercuryMesh);
        const mercuryRotationMesh = mercuryRotation.getMesh();
        mercurySystem.add(mercuryRotationMesh);
        const venusRotation = new Rotation(venusMesh);
        const venusRotationMesh = venusRotation.getMesh();
        venusSystem.add(venusRotationMesh);
        const earthRotation = new Rotation(earthMesh);
        const earthRotationMesh = earthRotation.getMesh();
        earthSystem.add(earthRotationMesh);
        const marsRotation = new Rotation(marsMesh);
        const marsRotationMesh = marsRotation.getMesh();
        marsSystem.add(marsRotationMesh);
        const jupiterRotation = new Rotation(jupiterMesh);
        jupiterSystem.add(jupiterRotation);

        // NOTE: Add solar system mesh GUI.
        await initGui();
        const solarSystemGui = gui.addFolder("solar system");
        solarSystemGui.add(mercuryRotationMesh, "visible").name("mercury").listen();
        solarSystemGui.add(venusRotationMesh, "visible").name("venus").listen();
        solarSystemGui.add(earthRotationMesh, "visible").name("earth").listen();
        solarSystemGui.add(marsRotationMesh, "visible").name("mars").listen();
        solarSystemGui.add(marsRotationMesh, "visible").name("jupiter").listen();

        // NOTE: Animate solar system at 60fps.
        const EARTH_YEAR = 2 * Math.PI * (1 / 60) * (1 / 60);
        const animate = () => {
        sunMesh.rotation.y += 0.001;
        mercurySystem.rotation.y += EARTH_YEAR * 1.2;
        venusSystem.rotation.y += EARTH_YEAR * 1.5;
        earthSystem.rotation.y += EARTH_YEAR;
        marsSystem.rotation.y += EARTH_YEAR * 0.5;
        jupiterSystem.rotation.y += EARTH_YEAR * 0.25;
        requestAnimationFrame(animate);
        };
        animate();
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
