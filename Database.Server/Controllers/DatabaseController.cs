using Database.Core.Interfaces;
using Database.Core.Models;
using Microsoft.AspNetCore.Mvc;

namespace Database.Server.Controllers
{
    [ApiController]
    [Route("api/cows")]
    public class CowController : ControllerBase
    {
        private readonly ICowDatabase _db;

        public CowController(ICowDatabase db)
        {
            _db = db;
        }

        [HttpPost]
        public IActionResult Insert([FromBody] CowDto dto)
        {
            var cow = new CowModel
            {
                Id = Guid.NewGuid(),
                Breed = dto.Breed,
                Age = dto.Age,
                Name = dto.Name,
                DnaData = dto.DnaData ?? Array.Empty<byte>()
            };
            _db.Insert(cow);
            return Created($"/api/cows/{cow.Id}", cow);
        }

        [HttpGet("{id:guid}")]
        public IActionResult Find(Guid id)
        {
            var cow = _db.Find(id);
            return cow != null ? Ok(cow) : NotFound();
        }

        [HttpGet("search")]
        public IActionResult FindBy([FromQuery] string breed, [FromQuery] int age)
        {
            var cows = _db.FindBy(breed, age);
            return Ok(cows);
        }

        [HttpPut]
        public IActionResult Update([FromBody] CowModel cow)
        {
            _db.Update(cow);
            return Ok();
        }

        [HttpDelete("{id:guid}")]
        public IActionResult Delete(Guid id)
        {
            var cow = _db.Find(id);
            if (cow == null) return NotFound();
            _db.Delete(cow);
            return NoContent();
        }
    }

    public class CowDto
    {
        public string Breed { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Name { get; set; } = string.Empty;
        public byte[]? DnaData { get; set; }
    }
}
