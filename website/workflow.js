$(window).load(function() {
    function createMarkers() {
      var defs = this.append('svg:defs')
      var paths = [
            { id: 0, name: 'circle', path: 'M 0, 0  m -5, 0  a 5,5 0 1,0 10,0  a 5,5 0 1,0 -10,0', viewbox: '-6 -6 12 12' }
          , { id: 1, name: 'square', path: 'M 0,0 m -5,-5 L 5,-5 L 5,5 L -5,5 Z', viewbox: '-5 -5 10 10' }
          , { id: 2, name: 'arrow', path: 'M 0,0 m -5,-5 L 5,0 L -5,5 Z', viewbox: '-5 -5 10 10' }
          , { id: 2, name: 'stub', path: 'M 0,0 m -1,-5 L 1,-5 L 1,5 L -1,5 Z', viewbox: '-1 -5 2 10' }
          ];
        var marker = defs.selectAll('marker')
          .data(paths)
          .enter()
          .append('svg:marker')
            .attr('id', function(d){ return 'marker_' + d.name })
            .attr('markerHeight', 4)
            .attr('markerWidth', 4)
            .attr('markerUnits', 'strokeWidth')
            .attr('orient', 'auto')
            .attr('refX', 0)
            .attr('refY', 0)
            .attr('viewBox', function(d){ return d.viewbox })
            .append('svg:path')
              .attr('d', function(d){ return d.path })
              .attr('fill', function(d,i) { return "rgb(37, 37, 37)"; return color(i)});

    }

    var margin = {top: 5, right: -25, bottom: 40, left: 0},
        width = 950 - margin.left - margin.right,
        height = 190 - margin.top - margin.bottom;
        //width = $("#workflow-container").width() - margin.left - margin.right,

    $("#workflow-container").width($(window).width());
    d3.select(window).on('resize', function() { $("#workflow-container").width($(window).width()); });

    var color = d3.scale.category20();

    var adapter = cola.d3adaptor()
        .linkDistance(80)
        .avoidOverlaps(true)
        .handleDisconnected(false)
        .size([width, height]);

    var svg = d3.select("#workflow-container").append("svg")
        .attr("width", width)
        .attr("height", height)
        .style("overflow", "scroll");

    createMarkers.call(svg);

    d3.json("graph.json", function (error, graph) {

        graph.groups.forEach(function (g) { g.padding = 0.01; });
        adapter
            .nodes(graph.nodes)
            .links(graph.links)
            .groups(graph.groups)
            .start(100, 0, 50, 50);

        var group = svg.selectAll(".group")
            .data(graph.groups)
          .enter().append("rect")
            .attr("rx", 8).attr("ry", 8)
            .attr("class", function(d) { return "group " + d.class })
            .attr("fill", function (d, i) { return d.color; return color(i); });

        var link = svg.selectAll(".link")
            .data(graph.links)
          .enter().append("line")
            .attr("class", "link")
            .attr('marker-end', function(d,i){ return 'url(#marker_arrow)' });

        var pad = 20;
        var node = svg.selectAll(".node")
            .data(graph.nodes)
          .enter().append("rect")
            .attr("class", function(d) { return "node " + d.class })
            .attr("width", function (d) { return d.width - 2 * pad; })
            .attr("height", function (d) { return d.height - 2 * pad; })
            .attr("rx", 5).attr("ry", 5)
            .attr("fill", function (d) { return d.color; return color(graph.groups.length); })
            .call(adapter.drag)
            .on('mouseup', function (d) {
                d.fixed = 0;
                adapter.alpha(1); // fire it off again to satify gridify
            });

        var label = svg.selectAll(".label")
            .data(graph.nodes)
           .enter().append("text")
            .attr("class", "label")
            .text(function (d) { return d.name; })
            .call(adapter.drag);

        var groupLabels = svg.selectAll(".groupLabel")
            .data(graph.groups)
           .enter().append("text")
            .attr("class", function(d,i) { return "label groupLabel group-" + i; })
            .attr("x", function(d) { return d.lx; })
            .attr("y", function(d) { return d.ly; })
            .text(function (d) { return d.name; });

        node.append("title")
            .text(function (d) { return d.name; });

        adapter.on("tick", function () {
            margin = 16;
            node.each(function (d) {
              return d.innerBounds = d.bounds.inflate(-margin); });
            link.each(function (d) {
                cola.vpsc.makeEdgeBetween(d, d.source.innerBounds, d.target.innerBounds, 5);
            });

            link.attr("x1", function (d) { return d.sx1; return d.source.x; })
                .attr("y1", function (d) { return d.sy1; return d.source.y; })
                .attr("x2", function (d) { return d.sx2; return d.arrowStart.x; })
                .attr("y2", function (d) { return d.sy2; return d.arrowStart.y; });

            node.attr("x", function (d) { return d.sx; return d.sx ? d.sx : d.x - d.width / 2 + pad; })
                .attr("y", function (d) { return d.sy; return d.sy ? d.sy : d.y - d.height / 2 + pad; });

            group.attr("x", function (d) { return d.sx; return d.bounds.x; })
                 .attr("y", function (d) { return d.sy; return d.bounds.y; })
                .attr("width", function (d) { return d.width; return d.bounds.width(); })
                .attr("height", function (d) { return d.height; return d.bounds.height(); });

            label.attr("x", function (d) { return d.lx; })
                 .attr("y", function (d) {
                     var h = this.getBBox().height;
                     return d.ly; //d.sy + h/4 + 30;
                 });
        });

        transfer = svg.append("g").attr("class", "transfer").attr("opacity", 0);
        transfer.append("rect")
           .attr("x", 277)
           .attr("y", 62)
           .attr("width", 60)
           .attr("height", 17)
           .attr("rx", 8)
           .attr("opacity", 0.5)
           .attr("fill", "#fff");
        transfer.append("text")
           .attr("x", 280)
           .attr("y", 75)
           .text("Transfer");

        legend = svg.append("g").attr("class", "legend").attr("opacity", 0);
        scidb = legend.append("g").attr("class", "scidb");
        scidb.append("rect")
           .attr("x", 0)
           .attr("y", 0)
           .attr("width", 30)
           .attr("height", 20)
           .attr("rx", 8)
           .attr("fill", "rgb(31, 120, 180)")
           .attr("opacity", 0.7);
        scidb.append("text")
            .attr("x", 33)
            .attr("y", 15)
            .text("SciDB");
        myria = legend.append("g").attr("class", "myria").attr("transform", "translate(80)");
        myria.append("rect")
           .attr("x", 0)
           .attr("y", 0)
           .attr("width", 30)
           .attr("height", 20)
           .attr("rx", 8)
           .attr("fill", "rgb(106, 61, 154)")
           .attr("opacity", 0.7);
        myria.append("text")
            .attr("x", 33)
            .attr("y", 15)
            .text("Myria");
        legend.attr("transform", function() { return "translate(" + (svg.node().getBBox().width / 2 - 20) + ", 125)" });
    });
});
