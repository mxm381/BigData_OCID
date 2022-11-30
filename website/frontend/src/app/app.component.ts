import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'OCID';
  longitude = "";
  lattitude = "";
  GSM_return = "";
  UMTS_return = "";
  CDMA_return = "";
  LTE_return = "";
  GSM_count = 0;
  UMTS_count = 0;
  CDMA_count = 0;
  LTE_count = 0;
  async action() {
    console.log("HI")
    await fetch("http://localhost:3000/" + this.longitude + "/" + this.lattitude)
        .then(response => response.json())
        .then(data =>  {
          console.log(data.data)
          this.GSM_count = 0
          this.UMTS_count = 0
          this.CDMA_count = 0;
          this.LTE_count = 0;
          for(var counter = 1; counter<data.data.length; counter++) {
            if(data.data[counter].radio == 'GSM') {
              this.GSM_count = this.GSM_count + 1
            }
            if(data.data[counter].radio == "UMTS") {
              this.UMTS_count = this.UMTS_count + 1
            }
            if(data.data[counter].radio == "CDMA") {
              this.CDMA_count = this.CDMA_count + 1
            }
            if(data.data[counter].radio == "LTE") {
              this.LTE_count = this.LTE_count + 1
            }
          }
          console.log("GSM" + this.GSM_count)
          console.log("UMTS" + this.UMTS_count)
          console.log("CDMA" + this.CDMA_count)
          console.log("LTE" + this.LTE_count)
          if(this.GSM_count < 2) {this.GSM_return = "poor"}
          if(this.GSM_count > 2) {this.GSM_return = "weak"}
          if(this.GSM_count > 5) {this.GSM_return = "okay"}
          if(this.GSM_count > 25) {this.GSM_return = "strong"} 
          if(this.UMTS_count < 2) {this.UMTS_return = "poor"}
          if(this.UMTS_count > 2) {this.UMTS_return = "weak"}
          if(this.UMTS_count > 5) {this.UMTS_return = "okay"}
          if(this.UMTS_count > 25) {this.UMTS_return = "strong"}      
          if(this.CDMA_count < 2) {this.CDMA_return = "poor"}
          if(this.CDMA_count > 2) {this.CDMA_return = "weak"}
          if(this.CDMA_count > 5) {this.CDMA_return = "okay"}
          if(this.CDMA_count > 25) {this.CDMA_return = "strong"}  
          if(this.LTE_count < 2) {this.LTE_return = "poor"}
          if(this.LTE_count > 2) {this.LTE_return = "weak"}
          if(this.LTE_count > 5) {this.LTE_return = "okay"}
          if(this.LTE_count > 25) {this.LTE_return = "strong"}    
        });
  }
}

