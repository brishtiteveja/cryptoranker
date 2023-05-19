import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls";

export default class SceneInit {
  constructor(fov = 50, camera, scene, stats, controls, renderer) {
    this.fov = fov;
    this.scene = scene;
    this.stats = stats;
    this.camera = camera;
    this.controls = controls;
    this.renderer = renderer;
    this.planets = [];
  }

  setSize() {
    const container = document.getElementById("myThreeJsCanvas");
    const containerWidth = container.clientWidth;
    const containerHeight = container.clientHeight;
  
    const canvasWidth = containerWidth * 0.95; // Adjust the percentage as needed
    const canvasHeight = containerHeight;
  
    this.camera.aspect = canvasWidth / canvasHeight;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(canvasWidth, canvasHeight);
  }

  initScene() {
    this.camera = new THREE.PerspectiveCamera(
      this.fov,
      window.innerWidth / window.innerHeight,
      1,
      1000
    );
    this.camera.position.z = 128;

    this.scene = new THREE.Scene();

    this.renderer = new THREE.WebGLRenderer({
      canvas: document.getElementById("myThreeJsCanvas"),
      antialias: true,
    });

    // this.setSize(); // Call the setSize method to set the initial size
    this.renderer.setSize(1200, 650);

    document.body.appendChild(this.renderer.domElement);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);

    let ambientLight = new THREE.AmbientLight(0xffffff, 0.2);
    this.scene.add(ambientLight);

    let pointLight = new THREE.PointLight(0xffffff, 1);
    pointLight.position.set(0, 0, 0);
    this.scene.add(pointLight);

    window.addEventListener("resize", () => this.onWindowResize());
  }

  animate() {
    requestAnimationFrame(this.animate.bind(this));
    this.planets.forEach((planet) => {
      planet.rotate();
    });

    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  }

  onWindowResize() {
    this.setSize(); // Call the setSize method when the window is resized
  }
}
