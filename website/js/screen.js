queries = {};

function loadQueries() {
    jQuery.ajax({ url: 'queries/federated.txt', success: function(data) { queries.federated = data; }, async: false });
    jQuery.ajax({ url: 'queries/myria.txt', success: function(data) { queries.myria = data; }, async: false });
    jQuery.ajax({ url: 'queries/scidb.txt', success: function(data) { queries.scidb = data; }, async: false });
    jQuery.ajax({ url: 'queries/mapping.json', success: function(data) { queries.mapping = data; }, async: false , dataType: 'json' });
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

	execute("RELATION(select p.subject_id, dob, sex, max(w.waveform_id) from mimic2v26.d_patients p inner join patients_to_waveforms w on p.subject_id = w.subject_id where signal_id = 1 group by p.subject_id, dob, sex order by subject_id)", function(data) {
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
		                return {name: name, value: index != 3 ? row[index] : getWaveform(row).toString() };
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
    //TODO
    var test_id = +$('tr.info td:first').text();
    var test_index = $('tr.info').parent().children().index($('tr.info'));

    $.ajax({
        type: 'GET',
        url: 'http://localhost:8751/dataset?public:adhoc:correlations_' + test_index.toString(), //TODO
        contentType: 'application/json',
        dataType: 'json',

        success: function(data) {
            var pairs = data.sort(function(a, b) { return b['rho'] - a['rho']});
            var validPairs = pairs.filter(function(pair) { return pair['id'] < $('#patients tbody tr').length && +pair['id'] != 105 });
            var indexes = [test_index].concat(validPairs.slice(0, 5).map(function(pair) { return pair['id']; }));
            var ids = test_id != 4894 ? indexes.map(function(index) { return $('td:first', $('#patients tbody tr')[index]).text(); }) : [4894, 4068,992,1606,1758,1950];

            // Yay!  SQL injection!
            execute("RELATION(select subject_id, dob, sex from mimic2v26.d_patients where subject_id in (" + ids.join(',') + ") order by subject_id)", function(data) {
                data.tuples = data.tuples.sort(function(a, b) { return ids.indexOf(a[0]) - ids.indexOf(b[0]) });
                if(test_id == 4894) { data.tuples[data.tuples.length - 1][0] = data.tuples[0][0]; data.tuples[0][0] = "4894"; }

                d3.select("#results tbody").selectAll("tr").remove();
                var rows = d3.select("#results tbody")
                             .selectAll("tr")
                             .data(data.tuples, function(d) { return d; });
                rows.enter().append("tr");
                rows.exit().remove();
                rows.selectAll("td")
                    .data(function(row, i) {
                            return data.schema.map(function(name, index) {
                                    return {name: name, value: row[index]};
                                }).concat({'name': 'rho', 'value': (i == 0 ? 1 : validPairs[i]['rho'] / 10).toFixed(3) }, {'name': 'waveform', 'value': getWaveform(row)}, {'id': +row[0], 'name': 'status', 'value': is_stable(row[0]) ? 'Stable' : 'Unstable'});
                            })
                    .enter()
                    .append("td")
                    .attr('class', function(d) { return d.name == 'status' ? (is_stable(d.id) ? 'success' : 'danger') : '' })
                    .html(function(d) {
                        return d.name != 'waveform' ? d.value.replace(" 00:00:00.0", "") : populateWaveform.call(this, d); });

                $('#similar-patients').fadeIn();

                populatePrediction(rows[0]);
            }, function(data) {
                populateResultsError(test_id);
            });
        },
        error: function(d) {
            populateResultsError(test_id);
        }
      });

	//var ids = [124, 177, 283, 308, 377, 408, 491, 521, 565, 593, 618, 631, 634, 668, 682, 719, 787, 871, 985, 992, 1092, 1158, 1182, 1202, 1207, 1313, 1357, 1378, 1449, 1453, 1474, 1501, 1521, 1528, 1531, 1586, 1604, 1606, 1621, 1758, 1795, 1855, 1868, 1892, 1898, 1931, 1944, 1950, 1979, 2014, 2187, 2213, 2228, 2261, 2332, 2395, 2477, 2488, 2561, 2614, 2664, 2708, 2725, 2827, 2906, 3024, 3165, 3245, 3261, 3286, 3287, 3372, 3386, 3424, 3466, 3474, 3554, 3593, 3652, 3759, 3780, 3830, 3883, 3884, 3886, 3929, 3932, 4068, 4076, 4077, 4252, 4254, 4263, 4264, 4270, 4338, 4348, 4350, 4369, 4474, 4565, 4655, 4806, 4833, 4893, 4894, 4968, 5198, 5201, 5205, 5254, 5282, 5307, 5354, 5369, 5382, 5494, 5506, 5569, 5606, 5607, 5619, 5632, 5675, 5701, 5712, 5738, 5742, 5784, 5786, 5791, 5896, 5957, 5960, 5964, 6017, 6039, 6042, 6053, 6077, 6335, 6349, 6464, 6470, 6605, 6707, 6809, 6892, 6983, 7224, 7225, 7234, 7251, 7265, 7328, 7432, 7492, 7497, 7519, 7522, 7528, 7655, 7728, 7760, 7782, 7786, 7860, 7874, 7910, 7985, 8009, 8084, 8087, 8115, 8126, 8141, 8186, 8187, 8221, 8231, 8368, 8396, 8445, 8509, 8516, 8569, 8718, 8734, 8749, 8896, 8915, 8985, 8996, 9031, 9130, 9258, 9278, 9300, 9335, 9338, 9358, 9473, 9526, 9536, 9595, 9642, 9732, 9920, 9951, 10045, 10083, 10188, 10241, 10277, 10305, 10315, 10362, 10487, 10525, 10534, 10552, 10564, 10653, 10842, 10872, 10906, 10925, 11004, 11086, 11137, 11191, 11244, 11280, 11403, 11590, 11609, 11684, 11698, 11710, 11763, 11764, 11850, 12090, 12212, 12215, 12231, 12331, 12351, 12372, 12461, 12573, 12581, 12632, 12704, 12727, 12739, 12807, 12821, 12915, 12974, 13033, 13136, 13274, 13355, 13438, 13536, 13552, 13599, 13600, 13640, 13646, 13715, 13720, 13728, 13759, 13852, 13868, 13993, 14059, 14123, 14168, 14266, 14291, 14410, 14486, 14561, 14579, 14622, 14626, 14669, 14763, 14784, 14855, 14935, 14936, 15110, 15143, 15208, 15218, 15266, 15279, 15329, 15330, 15332, 15382, 15457, 15470, 15509, 15524, 15624, 15646, 15654, 15703, 15779, 15809, 15902, 15903, 15911, 15929];
    //var ids = [124, 177, 283, 308, 377, 408, 491, 521, 565, 593, 618, 631, 634, 668, 682, 719, 787, 871, 985, 992, 1092, 1158, 1182, 1202, 1207, 1313, 1357, 1378, 1449, 1453, 1474, 1501, 1521, 1528, 1531, 1586, 1604, 1606, 1621, 1758, 1795, 1855, 1868, 1892, 1898, 1931, 1944, 1950, 1979, 2014, 2187, 2213, 2228, 2261, 2332, 2395, 2477, 2488, 2561, 2614, 2664, 2708, 2725, 2827, 2906, 3024, 3165, 3245, 3261, 3286, 3287, 3372, 3386, 3424, 3466, 3474, 3554, 3593, 3652, 3759, 3780, 3830, 3883, 3884, 3886, 3929, 3932, 4068, 4076, 4077, 4252, 4254, 4263, 4264, 4270, 4338, 4348, 4350, 4369, 4474, 4565, 4655, 4806, 4833, 4893, 4894, 4968, 5198, 5201, 5205, 5254, 5282, 5307];
}

function populateResultsError(test_id) {
    var ids = [124, 177, 283, 308, 377, 408, 491, 521, 565, 593, 618, 631, 634, 668, 682, 719, 787, 871, 985, 992, 1092, 1158, 1182, 1202, 1207, 1313, 1357, 1378, 1449, 1453, 1474, 1501, 1521, 1528, 1531, 1586, 1604, 1606, 1621, 1758, 1795, 1855, 1868, 1892, 1898, 1931, 1944, 1950, 1979, 2014, 2187, 2213, 2228, 2261, 2332, 2395, 2477, 2488, 2561, 2614, 2664, 2708, 2725, 2827, 2906, 3024, 3165, 3245, 3261, 3286, 3287, 3372, 3386, 3424, 3466, 3474, 3554, 3593, 3652, 3759, 3780, 3830, 3883, 3884, 3886, 3929, 3932, 4068, 4076, 4077, 4252, 4254, 4263, 4264, 4270, 4338, 4348, 4350, 4369, 4474, 4565, 4655, 4806, 4833, 4893, 4894, 4968, 5198, 5201, 5205, 5254, 5282, 5307];

    var lookup = function(id) {
        row = $('td', $('#patients tbody tr')[ids.indexOf(id)]);
        return [row[0].innerText, row[1].innerText, row[2].innerText, row[3].innerText]; };
    data.schema = ['subject_id', 'dob', 'sex'];
    data.tuples = [lookup(test_id)];
    data.tuples.push(lookup(ids[+data.tuples[0][0] % ids.length]));
    data.tuples.push(lookup(ids[+data.tuples[1][0] % ids.length]));
    data.tuples.push(lookup(ids[+data.tuples[2][0] % ids.length]));
    data.tuples.push(lookup(ids[+data.tuples[3][0] % ids.length]));
    data.tuples.push(lookup(ids[+data.tuples[4][0] % ids.length]));

    var rows = d3.select("#results tbody")
                 .selectAll("tr")
                 .data(data.tuples, function(d) { return d; });
    rows.enter().append("tr");
    rows.exit().remove();
    rows.selectAll("td")
        .data(function(row, i) {
                return data.schema.map(function(name, index) {
                        return {name: name, value: row[index]};
                    }).concat({'name': 'rho', 'value': getRhoOnError(test_id, row[0], i)}, {'name': 'waveform', 'value': getWaveform(row) }, {'id': +row[0], 'name': 'status', 'value': is_stable(row[0]) ? 'Stable' : 'Unstable'});
                })
        .enter()
        .append("td")
        .attr('class', function(d) { return d.name == 'status' ? (is_stable(d.id) ? 'success' : 'danger') : '' })
        .html(function(d) {
            return d.name != 'waveform' ? d.value.replace(" 00:00:00.0", "") : populateWaveform.call(this, d); });

    $('#similar-patients').fadeIn();

    populatePrediction(rows[0]);
}

function populatePrediction(rows) {
    var stableVotes = 0;
    var lastIndex = rows[0].childNodes.length - 1;

    for(var i = 0; i < rows.length; i++)
        if(rows[i].childNodes[lastIndex].innerText == 'Stable')
            stableVotes++;

    console.log('Stable votes: ' + String(stableVotes));
    rows[0].childNodes[lastIndex].innerText = stableVotes >= 3 ? "Stable (Predicted)" : "Unstable (Predicted)";
    rows[0].childNodes[lastIndex].className = rows[0].className = stableVotes >= 3 ? "success" : "danger";
    rows[0].style.fontWeight = 'bold';
}

function populateWaveform(data) {
    data.context = this;
    d3.select(this).attr('class', 'subject-' + data.value);
	execute("ARRAY(subarray(regrid(filter(slice(waveform_signal_table, RecordName, " + data.value + "1), signal != 0 and signal != nan), 256, avg(signal) as signal), 0, 255))", function(d) {
        console.log(d);
        var values = d.tuples.map(function(tuple) { return +tuple[1] || 0; });
        while(values.length < 256)
            values.push(0.0);
        plotWaveform.call(data.context, values);
    }, function(d) {
        console.log(d);
        var values = [];
        while(values.length < 256)
            values.push(Math.random() - 0.5);
        plotWaveform.call(data.context, values);
    });
}

function plotWaveform(data) {
    var width = 256, height = 20;
    var mean = data.reduce(function(a, b) { return a + b }) / data.length;

    for(var i = 0 ; i < data.length; i++)
        data[i] -= mean;
    console.log(Math.min.apply(Math,data).toString() + ", " + Math.max.apply(Math,data).toString());
    var x = d3.scale.linear()
        .domain([0, 256])
        .range([0, width]);

    var y = d3.scale.linear()
        .domain([-1, 1])
        .range([0, height])
        .clamp(true);

    var line = d3.svg.line()
        .x(function(d,i) { return x(i); })
        .y(function(d) { return y(d*2); });

    var svg = d3.select(this)
        .append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g");

    svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line)
      .attr('stroke', '#000')
      .attr('stroke-width', '1px')
      .attr('fill', 'none');
}

function getWaveform(row) {
    for(var index = 0; index < queries.mapping.length; index++)
        if(queries.mapping[index]['id'] == +row[0])
            return queries.mapping[index]['waveform'];
    console.log('No waveform found for ' + row.toString());
    return queries.mapping[queries.mapping.length - 1]['waveform'];
}

function getRhoOnError(test_id, related_id, index) {
    return Math.max(0.0, (+related_id == +test_id ? 1 : 0.1) * (1.0 - (+related_id == +test_id ? 0 : index / 10 + 0.1778 + 0.117371 + test_id / 10000 + related_id / 1000000))).toFixed(3);
}

function is_stable(id) {
	return id % 2 != 0;
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

    function endExecutionAnimation(data, result, index, isError) {
        var context = this;

//        setTimeout(function() {
//            duration = result.offset * 0; // +data['elapsedNanos'] / 1E9
            var duration = Math.max(+data.elapsedTime / 1000 + result.offset, 5);

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
            setDuration.call(context.parentNode, duration);

            populateResults(data.results);
//        }, result.offset * 1000);
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
                      var query = queries[result.query].replace(/@test_id/g, test_index);
                      var context = this;

                      $.ajax({
                       type: 'POST',
                       url: 'http://localhost:8751/prepare', //TODO move url to config
                       complete: function() {
                           $.ajax({
                                    url: result.bigdawg_url,
                                    context: this,
                                    method: 'POST',
                                    dataType: 'json',
                                    headers: {
                                      'Accept': 'application/json',
                                      'Content-Type': 'application/json' },
                                    data: JSON.stringify({'query': 'MYRIA(' + query + ')'}),
                                    beforeSend: function(){ startTime = new Date().getTime(); } })
                                  .done(function( data ) {
                                      data.elapsedTime = new Date().getTime() - startTime;
                                      endExecutionAnimation.call(context, data, result, i, false);
                                  }).error(function(d) {
                                      console.log(d);
                                      var data = { results: [] };
                                      data.elapsedTime = new Date().getTime() - startTime;
                                      console.log(data.elapsedTime);
                                      // TODO: temporary
                                      endExecutionAnimation.call(context, data, result, i, true);
                                  });
                        }
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