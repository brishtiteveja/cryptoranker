import * as THREE from "three";
import { FontLoader } from 'three/examples/jsm/loaders/FontLoader.js';
import { TextGeometry } from 'three/examples/jsm/geometries/TextGeometry.js';

export default class Planet {
  constructor(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile, planetInfo) {
    this.radius = radius;
    this.orbitRadius = orbitRadius;
    this.orbitAngle = Math.random() * 2 * Math.PI;
    this.orbitSpeed = orbitSpeed;
    this.planetSize = planetSize;
    this.color = color;
    this.textureFile = textureFile;
    this.planetInfo = planetInfo;

    this.mesh = null;
    this.orbitLine = null;
    this.textMesh = null;
  }

  getMesh(scene) {
    if (this.mesh === null) {
      // const geometry = new THREE.SphereGeometry(this.radius);
      // const material = new THREE.MeshPhongMaterial({
      //               map: new THREE.TextureLoader().load(this.textureFile),
      //               transparent: false,
      //               // opacity: 1,
      // });
      // // const material = new THREE.MeshBasicMaterial({ color: new THREE.Color(this.color) });
      // this.mesh = new THREE.Mesh(geometry, material);

      this.mesh = new THREE.Group();
      const material = new THREE.MeshPhongMaterial({
        map: new THREE.TextureLoader().load(this.textureFile),
        transparent: false,
        opacity: 1,
      });
  
      for (let i = 0; i < 4; i++) {
        const geometry = new THREE.SphereGeometry(this.radius, 32, 32, i * Math.PI / 2, Math.PI / 2);
        const sphereSegment = new THREE.Mesh(geometry, material);
        this.mesh.add(sphereSegment);
      }
  
      let x, y, z;
      if (this.planetInfo.name === 'btc') {
        x = 0;
        y = 0;
        z = 0;
        console.log(this.planetInfo.name)

      } else {
        x = this.orbitRadius * Math.cos(this.orbitAngle) + 1000;
        y = this.orbitRadius * Math.sin(this.orbitAngle) + 1000;
        z = 0;
      }

      this.mesh.position.set(
        x,
        y,
        z
      );

      this.mesh.rotation.set(
        Math.PI/2,
        -1.5*Math.PI/2,
        0
      );
  
      this.mesh.onClick = this.displayPlanetInfo.bind(this);
  
      // Display the name of the planet (the cryptocurrency)
      this.displayCryptoName(scene, this.planetInfo.name);
    }
  
    return this.mesh;
  }

  rotate() {
    this.orbitAngle += this.orbitSpeed;
    

    let x, y, z;
    

    if (this.planetInfo.name === 'btc') {
      x = 0;
      y = 0;
      z = 0;
    } else {
      x = this.orbitRadius * Math.cos(this.orbitAngle);
      y = this.orbitRadius * Math.sin(this.orbitAngle);
      z = 0;
    }

    this.mesh.position.set(
      x,
      y,
      z
    );

  }

  drawOrbit(scene, color, number) {
    const orbitGeometry = new THREE.BufferGeometry();
    const points = [];
    for (let i = 0; i < 100; i++) {
      const radian = (i / 100) * Math.PI * 2;
      points.push(
        new THREE.Vector3(
          Math.cos(radian) * this.orbitRadius,
          Math.sin(radian) * this.orbitRadius,
          0
        )
      );
    }
    orbitGeometry.setFromPoints(points);
    const orbitMaterial = new THREE.LineBasicMaterial({ color: this.color });
    this.orbitLine = new THREE.LineLoop(orbitGeometry, orbitMaterial);
    scene.add(this.orbitLine);

    const loader = new FontLoader();
    loader.load('fonts/helvetiker_regular.typeface.json', (font) => {
      const textMeshSize = 1; // Adjust the size of the text mesh
      const textHeight = 0;
      const textGeo = new TextGeometry(number.toString(), {
        font: font,
        size: textMeshSize,
        height: textHeight,
      });

      const textMaterial = new THREE.MeshBasicMaterial({ color: new THREE.Color(color) });
      this.textMesh = new THREE.Mesh(textGeo, textMaterial);

      // Calculate the position of the text mesh on the orbit
      const textPosition = new THREE.Vector3(
        this.orbitRadius + textMeshSize * 2, // Adjust the offset based on the text size
        0,
        0
      );

      this.textMesh.position.copy(textPosition);
      scene.add(this.textMesh);

      this.textMesh.onClick = this.displayPlanetInfo.bind(this);
    });
  }

  displayPlanetInfo() {
    const modal = document.createElement('div');
    modal.classList.add('fixed', 'inset-0', 'flex', 'items-center', 'justify-center', 'z-50');
  
    const modalContent = document.createElement('div');
    modalContent.classList.add('bg-blue-200', 'bg-opacity-75', 'rounded', 'shadow-lg', 'p-4', 'text-white');
  
    const title = document.createElement('h2');
    title.classList.add('text-2xl', 'font-bold', 'mb-2');
    title.textContent = this.planetInfo.name;
  
    const mass = document.createElement('p');
    mass.classList.add('text-lg', 'font-bold', 'mb-2', 'text-blue-500');
    mass.textContent = `Mass: ${this.planetInfo.mass}`;
  
    const distance = document.createElement('p');
    distance.classList.add('text-lg', 'font-bold', 'mb-2', 'text-green-500');
    distance.textContent = `Distance from Sun: ${this.planetInfo.distance}`;
  
    const orbitalPeriod = document.createElement('p');
    orbitalPeriod.classList.add('text-lg', 'font-bold', 'mb-2', 'text-yellow-500');
    orbitalPeriod.textContent = `Orbital Period: ${this.planetInfo.orbitalPeriod}`;
  
    modalContent.appendChild(title);
    modalContent.appendChild(mass);
    modalContent.appendChild(distance);
    modalContent.appendChild(orbitalPeriod);
  
    modal.appendChild(modalContent);
    document.body.appendChild(modal);
  
    modal.addEventListener('click', () => {
      modal.remove();
    });
  }

  displayCryptoName(scene, name) {
    const loader = new FontLoader();
    loader.load('fonts/helvetiker_regular.typeface.json', (font) => {
      const textGeo = new TextGeometry(name, {
        font: font,
        size: 3,  // adjust the size as needed
        height: 0,
      });
  
      const textMaterial = new THREE.MeshBasicMaterial({ color: this.color });
      const textMesh = new THREE.Mesh(textGeo, textMaterial);
  
      // Position the text on the planet
      textMesh.position.copy(this.mesh.position);
      textMesh.position.y += 0.25 * this.radius;  // offset the text above the planet
  
      scene.add(textMesh);
    });
  }
   
}
