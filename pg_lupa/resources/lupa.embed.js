let contentLockedToID = null;

function setContent(id, content, lock, processes1, processes2) {
  if (contentLockedToID && !lock) return;

  const alreadyLockedToThis = contentLockedToID && contentLockedToID === id;
  if (alreadyLockedToThis) {
    $("#context-info").html("");
    $(".context-highlight").removeClass("context-highlight");
    $(".process-highlight").removeClass(
      "process-primary-highlight process-secondary-highlight"
    );
    contentLockedToID = null;
    return;
  }

  $("#context-info").html(content);
  contentLockedToID = lock ? id : null;

  $(".context-highlight").removeClass("context-highlight");
  $("#" + id).addClass("context-highlight");

  $(".process-highlight").removeClass(
    "process-primary-highlight process-secondary-highlight"
  );
  if (processes1) {
    processes1.forEach((procid) => {
      $(document.getElementById(procid)).addClass(
        "process-highlight process-primary-highlight"
      );
    });
  }
  if (processes2) {
    processes2.forEach((procid) => {
      $(document.getElementById(procid)).addClass(
        "process-highlight process-secondary-highlight"
      );
    });
  }
}

function draw() {
  const width = $("#timeline").width() - 20;
  const data = JSON.parse(document.getElementById("data").textContent);

  const div = d3
    .select("body")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

  const svg = d3
    .select("#timeline")
    .append("svg")
    .attr("width", width)
    .attr("height", data.total_height);

  svg
    .selectAll()
    .data(data.processes)
    .enter()
    .append("rect")
    .attr("id", function (d) {
      return d.id;
    })
    .attr("x", 0)
    .attr("width", width)
    .attr("y", function (d) {
      return d.y;
    })
    .attr("height", function (d) {
      return d.height;
    })
    .attr("class", "pg_process");

  svg
    .selectAll()
    .data(data.statements)
    .enter()
    .append("rect")
    .attr("id", function (d) {
      return d.id;
    })
    .attr("class", "pg_stmt")
    .attr("x", function (d) {
      return (d.t_offset / data.total_duration) * width;
    })
    .attr("y", function (d) {
      return d.y;
    })
    .attr("width", function (d) {
      return (d.duration / data.total_duration) * width;
    })
    .attr("height", function (d) {
      return d.height;
    })
    .style("fill", function (d) {
      return d.colour;
    })
    .on("click", function (evt, d) {
      setContent(d.id, d.mouseover_content, true);
    })
    .on("mouseover", function (evt, d) {
      setContent(d.id, d.mouseover_content, false);
    })
    .on("mouseout", function (d) {
      setContent(null, "", false);
    });

  svg
    .selectAll()
    .data(data.events)
    .enter()
    .append("circle")
    .attr("id", function (d) {
      return d.id;
    })
    .attr("cx", function (d) {
      return (d.t_offset / data.total_duration) * width;
    })
    .attr("fill", function (d) {
      return d.colour;
    })
    .attr("cy", function (d) {
      return d.cy;
    })
    .attr("r", function (d) {
      return d.size;
    })
    .attr("class", "pg_event")
    .on("click", function (evt, d) {
      setContent(
        d.id,
        d.mouseover_content,
        true,
        d.primary_related_process_ids,
        d.secondary_related_process_ids
      );
    })
    .on("mouseover", function (evt, d) {
      setContent(
        d.id,
        d.mouseover_content,
        false,
        d.primary_related_process_ids,
        d.secondary_related_process_ids
      );
    })
    .on("mouseout", function (d) {
      setContent(null, "", false);
    });
}

draw();
