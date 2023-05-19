import * as THREE from "three";

export default class Planet {
  constructor(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile) {
    this.radius = radius;
    this.orbitRadius = orbitRadius;
    this.orbitAngle = Math.random() * 2 * Math.PI;
    this.orbitSpeed = orbitSpeed;
    this.planetSize = planetSize;
    this.color = color;
    this.textureFile = textureFile;
  }

  getMesh() {
    if (this.mesh === undefined || this.mesh === null) {
      const geometry = new THREE.SphereGeometry(this.radius);
      // const texture = new THREE.TextureLoader().load(this.textureFile);
      // const material = new THREE.MeshBasicMaterial({ map: texture });
      this.mesh = new THREE.Mesh(geometry) //, material);
      
      this.mesh.position.set(
        this.orbitRadius * Math.cos(this.orbitAngle),
        this.orbitRadius * Math.sin(this.orbitAngle),
        0
      );
    }
    return this.mesh;
  }

  rotate() {
    this.orbitAngle += this.orbitSpeed;
    this.mesh.position.set(
      this.orbitRadius * Math.cos(this.orbitAngle),
      this.orbitRadius * Math.sin(this.orbitAngle),
      0
    );
  }

  drawOrbit() {
    const orbitGeometry = new THREE.CircleGeometry(this.orbitRadius, 100);
    orbitGeometry.rotateX(Math.PI / 2);
    const colors = {
      'red': 0xff0000,
      'blue': 0x0000ff,
      'green': 0x008000,
      'yellow': 0xffff00,
      'purple': 0x800080,
      'orange': 0xffa500,
      'cyan': 0x00ffff,
      'magenta': 0xff00ff,
      'lime': 0x00ff00,
      'pink': 0xffc0cb
    };
    const randomColor = () => {
      const colorNames = Object.keys(colors);
      const randomColorName = colorNames[Math.floor(Math.random() * colorNames.length)];
      return colors[randomColorName];
    };
    const orbitMaterial = new THREE.LineBasicMaterial({ color: randomColor() });
    const orbitLine = new THREE.Line(orbitGeometry, orbitMaterial);
    return orbitLine;
  }
}
