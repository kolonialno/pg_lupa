const unfocusedContent = $("#context-info").html();
let contentLockedToID = null;

function unfocus() {
  $("#context-info").html(unfocusedContent);
  $(".context-highlight").removeClass("context-highlight");
  $(".process-highlight").removeClass(
    "process-primary-highlight process-secondary-highlight"
  );
  contentLockedToID = null;
}

function setSidebarContent(id, content, lock, processes1, processes2) {
  if (contentLockedToID && !lock) return;

  const alreadyLockedToThis = contentLockedToID && contentLockedToID === id;

  if (alreadyLockedToThis || !id) {
    unfocus();
    return;
  }

  $("#context-info").html(content);
  contentLockedToID = lock && id ? id : null;

  $(".context-highlight").removeClass("context-highlight");

  if (id) {
    $("#" + id).addClass("context-highlight");
  }

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

function resetSidebarContent(force) {
  setSidebarContent(null, "", force);
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
      return (d.t_offset / data.total_duration_seconds) * width;
    })
    .attr("y", function (d) {
      return d.y;
    })
    .attr("width", function (d) {
      return (d.duration / data.total_duration_seconds) * width;
    })
    .attr("height", function (d) {
      return d.height;
    })
    .style("fill", function (d) {
      return d.colour;
    })
    .on("click", function (evt, d) {
      setSidebarContent(d.id, d.mouseover_content, true);
      evt.stopPropagation();
    })
    .on("mouseover", function (evt, d) {
      setSidebarContent(d.id, d.mouseover_content, false);
      evt.stopPropagation();
    })
    .on("mouseout", function (d) {
      resetSidebarContent(false);
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
      return (d.t_offset / data.total_duration_seconds) * width;
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
      setSidebarContent(
        d.id,
        d.mouseover_content,
        true,
        d.primary_related_process_ids,
        d.secondary_related_process_ids
      );
      evt.stopPropagation();
    })
    .on("mouseover", function (evt, d) {
      setSidebarContent(
        d.id,
        d.mouseover_content,
        false,
        d.primary_related_process_ids,
        d.secondary_related_process_ids
      );
      evt.stopPropagation();
    })
    .on("mouseout", function (d) {
      resetSidebarContent(false);
    });
}

$(document).on("click", function () {
  resetSidebarContent(true);
});

draw();
