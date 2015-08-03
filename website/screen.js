query = '-- exec myriax \n\
const test_vector_id: 1; \n\
const bins: 10; \n\
vectors = scan(public:adhoc:relation600x256); \n\
\n\
const alpha: 1.0;\n\
\n\
def log2(x): log(x) / log(2);\n\
def mod2(x): x - int(x/2)*2;\n\
def iif(expression, true_value, false_value):\n\
    case when expression then true_value\n\
         else false_value end;\n\
def bucket(x, high, low): greater(least(int((bins-1) * (x - low) / iif(high != low, high - low, 1)),\n\
                                bins - 1), 0);\n\
def difference(current, previous, previous_time, time):\n\
    iif(previous_time >= 0,\n\
        (current - previous) * iif(previous_time < time, 1, -1),\n\
        current);\n\
def idf(w_ij, w_ijN, N): log(N / w_ijN) * w_ij;\n\
\n\
symbols = empty(id:int, index:int, value:int);\n\
\n\
uda HarrTransformGroupBy(alpha, time, x) {\n\
  [0.0 as coefficient, 0.0 as _sum, 0 as _count, -1 as _time];\n\
  [difference(x, coefficient, _time, time), _sum + x, _count + 1, time];\n\
  [coefficient, _sum / int(_count * alpha)];\n\
};\n\
\n\
iterations = [from vectors where id = test_vector_id emit 0 as i, int(ceil(log2(count(*)))) as total];\n\
\n\
do\n\
    groups = [from vectors emit\n\
                     id,\n\
                     int(floor(time/2)) as time,\n\
                     HarrTransformGroupBy(alpha, time, value) as [coefficient, mean]];\n\
\n\
    coefficients = [from groups emit id, coefficient];\n\
    range = [from vectors emit max(value) - min(value) as high, min(value) - max(value) as low];\n\
\n\
    histogram = [from coefficients, range\n\
                 emit id,\n\
                      bucket(coefficient, high, low) as index,\n\
                      count(bucket(coefficient, high, low)) as value];\n\
    symbols = symbols + [from histogram, iterations emit id, index + i*bins as index, value];\n\
    vectors = [from groups emit id, time, mean as value];\n\
\n\
    iterations = [from iterations emit $0 + 1, $1];\n\
while [from iterations emit $0 < $1];\n\
\n\
sink(symbols);';

queries = {};

function loadQueries() {
    jQuery.ajax({
             url: 'federated_query.txt',
             success: function(data) {
                 alert(data);
                 },
             async: false
        });
}

function populateStrategy() {
	$('#execute-myria')
		.on('mouseover', highlight_myria)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-myria')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
	$('#execute-scidb')
		.on('mouseover', highlight_scidb)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-scidb')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
	$('#execute-hybrid-csv')
		.on('mouseover', highlight_hybrid)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-hybridscidbmyriacsv')
						   			.each(function(d, i) { d3.select(this)
						   									 .on('click')
						   									 .call(this, d, i); }); });
	$('#execute-hybrid-binary')
		.on('mouseover', highlight_hybrid)
		.on('mouseout', function() { highlight_system() })
		.on('click', function() { d3.select('.execute-hybridscidbmyriabinary')
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
			           .on('click', function(d) {
			           		$('.info').attr('class', '');
			           		d3.select(this).attr('class', 'info');
			           })

		var cells = rows.selectAll("td")
		        .data(function(row) {
		            return data.schema.map(function(name, index) {
		                return {name: name, value: row[index]};
		            });
		        })
		        .enter()
		        .append("td")
	            .html(function(d) { return d.value.replace(" 00:00:00.0", ""); });
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
	    console.log(data.tuples);
		var cells = rows.selectAll("td")
		        .data(function(row) {
		            return data.schema.map(function(name, index) {
		                	return {name: name, value: row[index]};
			            }).concat({'name': 'waveform', 'value': ''}, {'id': +row[0], 'name': 'status', 'value': is_stable(row[0]) ? 'Stable' : 'Unstable'});
			        })
		        .enter()
		        .append("td")
		        .attr('class', function(d) { return d.name == 'status' ? (is_stable(d.id) ? 'success' : 'danger') : '' })
	            .html(function(d) { console.log(d.value); return d.value.replace(" 00:00:00.0", ""); });

	    $('#similar-patients').fadeIn();
	});
}

function is_stable(id) {
	return id % 4 != 0;
}

function execute(query, callback, error) {
   url="http://localhost:8080/bigdawg/query"

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
                   .attr("class", function(d) { return 'execute-' + d.title.toLowerCase().replace(/[^\w]/g, ''); })
                   .style("cursor", "pointer")
                   .on("mouseover", highlight_system)
                   .on("mouseout", function() { highlight_system() })
                   .on("click", function(result, i) {
                   	  $("#performance").fadeIn();
                      result.status = "EXECUTING";
                      window.current_system = result;
                      setDuration.call(this.parentNode, 0);

                      d3.selectAll("text.subtitle").filter(function(d,si) { return i == si; }).transition().attr('fill', '#fff').each("end", function() { d3.select(this).text('(Executing)').transition().attr('fill', '#000'); });
                      pulse.call(this.parentNode);

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
                         	context = this;

                         	setTimeout(function() {
	                         	duration = result.offset * 0; // +data['elapsedNanos'] / 1E9

	                            window.current_system = undefined;
	                            result.status = undefined;
	                            highlight_none();
	                            d3.select(context.parentNode).transition().attr('fill', '#000');
	                            d3.selectAll("text.subtitle").filter(function(d,si) { return i == si; }).transition().attr('fill', '#fff').each("end", function() { d3.select(this).text('Click to Execute').transition().attr('fill', '#999'); });
	                            setDuration.call(context.parentNode, duration + result.offset);

	                            populateResults(data.results);
	                        }, result.offset * 1000);
                         }).error(function(d) { console.log(d) });
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
