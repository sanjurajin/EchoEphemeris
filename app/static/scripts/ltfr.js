
const ctx{{ loop.index }} = document.getElementById('chart{{ loop.index }}').getContext('2d');
const chart{{ loop.index }} = new Chart(ctx{{ loop.index }}, {
    type: 'line',
    data: {
        labels: {{ chart.labels | tojson }},
        datasets: [{
            label: '{{ chart.datasets[0].label }}',
            data: {{ chart.datasets[0].data | tojson }},
            borderColor: 'rgb(75, 192, 192)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.8,
            // pointStyle: 'crossRot',
            borderWidth: 2, // Adjust line thickness
            pointRadius: 3, // Adjust point size
            pointStyle: false,
            // fill: false,
            fill: true,
                fill: {
                    target: 'origin',
                    above: 'rgb(0,255, 136, 0.385)',   // Area will be red above the origin
                    below: 'rgba(255, 0,0, 0.200)',    // And blue below the origin
                 }

            }]
            },
    options: {
        // maintainAspectRatio: false,
        animation: {
                easing: 'easeInOutQuad',
                duration: 1000,
            },
        responsive: true,
        interaction: {
                intersect: false,
                mode: 'nearest',
                axis : 'xy',
                },
        scales: {
            x: {
                display: false // Hides x-axis labels
                },
                
            y: {
                display: true, // Hides y-axis labels
                
                beginAtZero: true,
                grid: {
                    color: 'rgba(0, 0, 0, 0.1)' // Light grid color
                    },
                
                ticks: {
                    callback: function(value) {
                        return value === 0 ? '0' : value;
                            }
                        }
                }
                },
        plugins: {
            
            legend: {
                // display: true, // Hides the legend
                // position = 'left',
                
                display: true,
                    position: 'top',
                    align: 'start',
                    labels: {
                        // boxWidth: 30,
                        // color:'darkgray',
                        font: {
                            weight: 'bolder',
                            family: 'sans-serif',
                            size: 12,
                            
                        }
                    },
                    },
            title: {
                // display: true,
                    // text: (ctx) => 'Point Style: ' + ctx.chart.data.datasets[0].pointStyle,
                    },
            afterDraw: function(chart) {
                const ctx = chart.ctx;
                const chartArea = chart.chartArea;

                // Draw horizontal zero line
                ctx.save();
                ctx.beginPath();
                ctx.moveTo(chartArea.left, chart.scales.y.getPixelForValue(0)); // Get pixel for zero value
                ctx.lineTo(chartArea.right, chart.scales.y.getPixelForValue(0));
                ctx.lineWidth = 2;
                ctx.strokeStyle = 'red';
                ctx.stroke();
                ctx.restore();

                    }
                }
            }
});

