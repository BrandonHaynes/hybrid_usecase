function highlight_system(d) {
  console.log("highlight_system");
  console.log(!window.current_system);
  console.log(d ? d.title : null);
  if(!window.current_system)
    switch(d ? d.title : null) {
      case "SciDB":
        return highlight_scidb();

      case "Myria":
        return highlight_myria();

      case "Hybrid SciDB → Myria (CSV)":
      case "Hybrid SciDB → Myria (Binary)":
        return highlight_hybrid();

      case null:
        return highlight_none();
    }
}

function highlight_scidb() {
  d3.selectAll(".group").transition().duration(1000).attr("fill", "rgb(31, 120, 180)");
  d3.selectAll(".node").transition().duration(1000).attr("fill", "rgb(166, 206, 227)");
  d3.selectAll(".transfer").transition().duration(1000).attr("opacity", 0);
  d3.selectAll(".legend").transition().duration(1000).attr("opacity", 1);
}

function highlight_myria() {
  d3.selectAll(".group").transition().duration(1000).attr("fill", "rgb(106, 61, 154)");
  d3.selectAll(".node").transition().duration(1000).attr("fill", "rgb(202, 178, 214)");
  d3.selectAll(".transfer").transition().duration(1000).attr("opacity", 0);
  d3.selectAll(".legend").transition().duration(1000).attr("opacity", 1);
}

function highlight_hybrid() {
  var scidb_groups = function(d, i) { return i < 1 };
  var myria_groups = function(d, i) { return i >= 1 };
  var scidb_nodes = function(d, i) { return i < 2 };
  var myria_nodes = function(d, i) { return i >= 2 };

  d3.selectAll(".group").filter(scidb_groups).transition().duration(1000).attr("fill", "rgb(31, 120, 180)");
  d3.selectAll(".node").filter(scidb_nodes).transition().duration(1000).attr("fill", "rgb(166, 206, 227)");

  d3.selectAll(".group").filter(myria_groups).transition().duration(1000).attr("fill", "rgb(106, 61, 154)");
  d3.selectAll(".node").filter(myria_nodes).transition().duration(1000).attr("fill", "rgb(202, 178, 214)");

  d3.selectAll(".transfer").transition().duration(1000).attr("opacity", 1);
  d3.selectAll(".legend").transition().duration(1000).attr("opacity", 1);
}

function highlight_none() {
  d3.selectAll(".group").transition().duration(1000).attr("fill", function(d) { return d.color });
  d3.selectAll(".node").transition().duration(1000).attr("fill", function(d) { return d.color });
  d3.selectAll(".transfer").transition().duration(1000).attr("opacity", 0);
  d3.selectAll(".legend").transition().duration(1000).attr("opacity", 0);
}


$(function() {
  function setDuration(duration, transition) {
     d3.select(this).datum(function(d) {
       d.measures[1] = +duration;
       return d;
     }).call(chart.duration(transition || 1000));
  }

  function pulse() {
    (function repeat() {
            d3.select(this)
              .filter(function(d) { return d.status; })
              .transition()
              .duration(750)
              .attr("fill", d3.rgb(239, 59, 44))
              .transition()
              .duration(750)
              .attr('fill', d3.rgb(103, 0, 13))
              .each("end", repeat);
          }).call(this);
  }

  var margin = {top: 5, right: 60, bottom: 20, left: 220},
      width = $(window).width() - margin.left - margin.right,
      height = 50 - margin.top - margin.bottom;

  var chart = d3.bullet()
                .width(width)
                .height(height);

  d3.json("bullets.json", function(error, data) {
    if (error) throw error;

    var svg = d3.select("#execution")
                .selectAll("svg")
                .data(data)
                  .enter().append("svg")
                    .attr("class", "bullet")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                  .append("g")
                    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
                    .call(chart);

    var title = svg.append("g")
                   .style("text-anchor", "end")
                   .attr("transform", "translate(-6," + height / 2 + ")")
                   .style("cursor", "pointer")
                   .on("mouseover", highlight_system)
                   .on("mouseout", function() { highlight_system() })
                   .on("click", function(result, i) {
                      result.status = "EXECUTING";
                      window.current_system = result;
                      setDuration.call(this.parentNode, 0);

                      d3.selectAll("text.subtitle").filter(function(d,si) { return i == si; }).transition().attr('fill', '#fff').each("end", function() { d3.select(this).text('(Executing)').transition().attr('fill', '#000'); });
                      pulse.call(this.parentNode);

                      $.ajax({ url: result.url, context: this })
                         .done(function( data ) {
                           window.current_system = undefined;
                           result.status = undefined;
                           highlight_none();
                           d3.select(this.parentNode).transition().attr('fill', '#000');
                           d3.selectAll("text.subtitle").filter(function(d,si) { return i == si; }).transition().attr('fill', '#fff').each("end", function() { d3.select(this).text('Click to Execute').transition().attr('fill', '#999'); });
                           setDuration.call(this.parentNode, +new RegExp(result.expression).exec(data)[1]);
                         });
                   });

    title.append("text")
        .attr("class", "title")
        .style("cursor", "pointer")
        .text(function(d) { return d.title; });

    title.append("text")
        .attr("class", "subtitle")
        .attr("dy", "1.1em")
        .style("cursor", "pointer")
        .text(function(d) { return "Click to Execute"; })
        .attr('fill', '#999')
        .on('mouseover', function(d) {
          d3.select(this).transition().attr('fill', '#000'); })
        .on('mouseout', function(d) {
          d3.select(this).transition().attr('fill', '#999'); });
  });
});
