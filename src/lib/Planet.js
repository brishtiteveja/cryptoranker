import * as THREE from "three";

export default class Planet {
  constructor(radius, orbitRadius, orbitSpeed, planetSize, color, textureFile) {
    this.radius = radius;
    this.orbitRadius = orbitRadius;
    this.orbitSpeed = orbitSpeed;
    this.planetSize = planetSize;
    this.color = color;
    this.textureFile = textureFile;
  }

  getMesh() {
    if (this.mesh === undefined || this.mesh === null) {
      const geometry = new THREE.SphereGeometry(this.radius);
      const texture = new THREE.TextureLoader().load(this.textureFile);
      const material = new THREE.MeshBasicMaterial({ map: texture });
      this.mesh = new THREE.Mesh(geometry, material);
      
      const initialAngle = Math.random() * 2 * Math.PI;
      this.mesh.position.set(
        this.orbitRadius * Math.cos(initialAngle),
        this.orbitRadius * Math.sin(initialAngle),
        0
      );
    }
    return this.mesh;
  }
}
