import * as THREE from "three";
import { FontLoader } from 'three/examples/jsm/loaders/FontLoader.js';
import { TextGeometry } from 'three/examples/jsm/geometries/TextGeometry.js';

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
      this.mesh = new THREE.Mesh(geometry);
      
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

  // drawOrbit(scene, color, number) {
  //   const orbitGeometry = new THREE.BufferGeometry();
  //   const points = [];
  //   for (let i = 0; i < 100; i++) {
  //     const radian = (i / 100) * Math.PI * 2;
  //     points.push(
  //       new THREE.Vector3(
  //         Math.cos(radian) * this.orbitRadius,
  //         Math.sin(radian) * this.orbitRadius,
  //         0
  //       )
  //     );
  //   }
  //   orbitGeometry.setFromPoints(points);
  //   const orbitMaterial = new THREE.LineBasicMaterial({ color: new THREE.Color(color) });
  //   const orbitLine = new THREE.LineLoop(orbitGeometry, orbitMaterial);
  //   scene.add(orbitLine);

  //   // Create font loader
  //   const loader = new FontLoader();
  //   loader.load( 'fonts/helvetiker_regular.typeface.json', function ( font ) {
  //     const textMeshSize = 10;
  //     const textHeight = 0;
  //     const textGeo = new TextGeometry( number.toString(), {
  //       font: font,
  //       size: textMeshSize,
  //       height: textHeight,
  //       curveSegments: 12,
  //       bevelEnabled: true,
  //       bevelThickness: 10,
  //       bevelSize: 8,
  //       bevelOffset: 0,
  //       bevelSegments: 5
  //     } );

  //     const textMaterial = new THREE.MeshPhongMaterial( { color: 0xff0000 } );
  //     const mesh = new THREE.Mesh( textGeo, textMaterial );
  //     mesh.position.set(
  //       (this.orbitRadius + 100) * Math.cos(this.orbitAngle),
  //       (this.orbitRadius + 100) * Math.sin(this.orbitAngle),
  //       0
  //     );
  //     scene.add( mesh );
  //   }.bind(this) );
  // }

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
    const orbitMaterial = new THREE.LineBasicMaterial({ color: new THREE.Color(color) });
    const orbitLine = new THREE.LineLoop(orbitGeometry, orbitMaterial);
    scene.add(orbitLine);
  
    const loader = new FontLoader();
    loader.load('fonts/helvetiker_regular.typeface.json', (font) => {
      const textMeshSize = 1; // Adjust the size of the text mesh
      const textHeight = 0;
      const textGeo = new TextGeometry(number.toString(), {
        font: font,
        size: textMeshSize,
        height: textHeight,
        // curveSegments: 12,
        // bevelEnabled: true,
        // bevelThickness: 10,
        // bevelSize: 8,
        // bevelOffset: 0,
        // bevelSegments: 5
      });
  
      const textMaterial = new THREE.MeshBasicMaterial({ color: new THREE.Color(color) });
      const textMesh = new THREE.Mesh(textGeo, textMaterial);
  
      // Calculate the position of the text mesh on the orbit
      const textPosition = new THREE.Vector3(
        this.orbitRadius + textMeshSize * 2, // Adjust the offset based on the text size
        0,
        0
      );
  
      textMesh.position.copy(textPosition);
      scene.add(textMesh);
    });
  }
}
