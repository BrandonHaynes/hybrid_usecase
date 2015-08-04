queries = {};

function loadQueries() {
    jQuery.ajax({ url: 'queries/federated.txt', success: function(data) { queries.federated = data; }, async: false });
    jQuery.ajax({ url: 'queries/myria.txt', success: function(data) { queries.myria = data; }, async: false });
    jQuery.ajax({ url: 'queries/scidb.txt', success: function(data) { queries.scidb= data; }, async: false });
}

function populateStrategy() {
	$('#execute-myria')
		.on('mouseover', highlight_myria)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-myria.benchmark')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
	$('#execute-scidb')
		.on('mouseover', highlight_scidb)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-scidb.benchmark')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
	$('#execute-hybrid-csv')
		.on('mouseover', highlight_hybrid)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-scidbmyriatext.benchmark')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });

	$('#execute-hybrid-binary')
		.on('mouseover', highlight_hybrid)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-scidbmyriabinary.benchmark')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
}

function populatePatients() {

	execute("RELATION(select p.subject_id, dob, sex, min(w.waveform_id) from mimic2v26.d_patients p inner join patients_to_waveforms w on p.subject_id = w.subject_id where signal_id = 1 group by p.subject_id, dob, sex order by subject_id)", function(data) {
	    var rows = d3.select("#patients tbody")
	                 .selectAll("tr")
	                 .data(data.tuples)
	                   .enter()
	                   .append("tr")
			           .attr('class', function(_, index)
			           		{ return index == 0 ? 'info' : ''; })
			           .on('click', function() {
			           		$('.info').attr('class', '');
			           		d3.select(this).attr('class', 'info');
			           });

		rows.selectAll("td")
		    .data(function(row) {
		            return data.schema.map(function(name, index) {
		                return {name: name, value: row[index]};
		            });
		        })
		    .enter()
		    .append("td")
	        .html(function(d) { return d.value.replace(" 00:00:00.0", ""); });
	});


//TODO remove
$("#patients tbody tr").click(function() {
			           		$('.info').attr('class', '');
			           		d3.select(this).attr('class', 'info');
			           });
}

function populateResults(ids) {
	ids = [1000, 1010, 1038, 1039, 1060];

	// Yay!  SQL injection!
	execute("RELATION(select subject_id, dob, sex from mimic2v26.d_patients where subject_id in (" + ids.join(',') + ") order by subject_id limit 5)", function(data) {
	    var rows = d3.select("#results tbody")
	                 .selectAll("tr")
	                 .data(data.tuples)
	                   .enter()
	                   .append("tr");
		rows.selectAll("td")
		    .data(function(row) {
		            return data.schema.map(function(name, index) {
		                	return {name: name, value: row[index]};
			            }).concat({'name': 'waveform', 'value': ''}, {'id': +row[0], 'name': 'status', 'value': is_stable(row[0]) ? 'Stable' : 'Unstable'});
			        })
		    .enter()
		    .append("td")
		    .attr('class', function(d) { return d.name == 'status' ? (is_stable(d.id) ? 'success' : 'danger') : '' })
	        .html(function(d) { return d.value.replace(" 00:00:00.0", ""); });

	    $('#similar-patients').fadeIn();
	});
}

function is_stable(id) {
	return id % 4 != 0;
}

function execute(query, callback, error) {
   var url = "http://localhost:8080/bigdawg/query";

   $.ajax({
      type: 'POST',
      url: url,
      crossDomain: true,
      contentType: 'application/json',
      processData: false,
      dataType: 'json',
      data: JSON.stringify({"query": query}),

      success: callback,
      error: error
    });
}

$(function() {
    loadQueries();
	populatePatients();
	populateStrategy();
});


/******************************************************/

function highlight_system(d) {
  if(!window.current_system)
    switch(d ? d.title : null) {
      case "SciDB":
        return highlight_scidb();

      case "Myria":
        return highlight_myria();

      case "Hybrid SciDB → Myria (text transfer)":
      case "Hybrid SciDB → Myria (binary transfer)":
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

  function pulse(className) {
    (function repeat() {
            d3.selectAll('.' + className)
              .transition()
              .duration(750)
              .attr("fill", d3.rgb(239, 59, 44))
              .style("color", d3.rgb(239, 59, 44))
              .transition()
              .duration(750)
              .attr('fill', d3.rgb(103, 0, 13))
              .style('color', d3.rgb(103, 0, 13))
              .each("end", repeat);
          }).call(this);
  }

    function startExecutionAnimation(result, index) {
      $("#performance").fadeIn();
      result.status = "EXECUTING";
      window.current_system = window.last_system = result;
      setDuration.call(this.parentNode, 0);
      pulse.call(this.parentNode, result.pulseClassName);

      d3.selectAll("text.subtitle")
        .filter(function(d,si) { return index == si; })
        .transition()
        .attr('fill', '#fff')
        .each("end", function() {
          d3.select(this)
            .text('(Executing)')
            .transition()
            .attr('fill', '#000'); });
    }

    function endExecutionAnimation(data, result, index) {
        var context = this;

        setTimeout(function() {
            duration = result.offset * 0; // +data['elapsedNanos'] / 1E9

            window.current_system = undefined;
            result.status = undefined;
            highlight_none();
            d3.selectAll('.' + result.pulseClassName).transition().attr('fill', '#000').style('color', '#000');
            d3.selectAll("text.subtitle")
              .filter(function(d,si) { return index == si; })
              .transition()
              .attr('fill', '#fff')
              .each("end", function() {
                    d3.select(this).text('Click to Execute')
                      .transition()
                      .attr('fill', '#999'); });
            setDuration.call(context.parentNode, duration + result.offset);

            populateResults(data.results);
        }, result.offset * 1000);
   }

  //var margin = {top: 5, right: 60, bottom: 20, left: 220},
      //width = $(window).width()/2 - margin.left - margin.right,
      //height = 50 - margin.top - margin.bottom;
  var margin = {top: 5, right: 80, bottom: 20, left: 220},
      width = $(window).width()/2 - margin.left - margin.right,
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
                   .attr("class", function(d) { return 'benchmark execute-' + d.title.toLowerCase().replace(/[^\w]/g, ''); })
                   .style("cursor", "pointer")
                   .on("mouseover", highlight_system)
                   .on("mouseout", function() { highlight_system() })
                   .on("click", function(result, i) {
                      startExecutionAnimation.call(this, result, i);
                      var test_id = +$('tr.info td:first').text();
                      var test_index = $('tr.info').parent().children().index($('tr.info'));
                      var query = queries[result.query].replace('@test_id', test_index);

                      $.ajax({
                            url: result.bigdawg_url,
                            context: this,
                            method: 'POST',
                            dataType: 'json',
                            headers: {
                              'Accept': 'application/json',
                              'Content-Type': 'application/json' },
                            data: JSON.stringify({'query': 'MYRIA(' + query + ')'}) })
                          .done(function( data ) {
                            endExecutionAnimation.call(this, data, result, i);
                          }).error(function(d) {
                              console.log(d);
                              var data = { results: [] };
                              // TODO: temporary
                              endExecutionAnimation.call(this, data, result, i);
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
        .text(function() { return "Click to Execute"; })
        .attr('fill', '#999')
        .on('mouseover', function() {
          d3.select(this).transition().attr('fill', '#000'); })
        .on('mouseout', function() {
          d3.select(this).transition().attr('fill', '#999'); });
  });
});